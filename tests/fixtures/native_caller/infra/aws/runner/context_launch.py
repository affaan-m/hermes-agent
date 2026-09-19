"""Executable V2 request/observation guard; no SDK import or provider call at import.

Canonical admission, durable request receipt and unknown-capacity retention remain
owned by the existing dispatcher. This guard cannot confer admission authority.
"""
import re
import threading
from .context_bootstrap import parse_coordinates
from .context_exchange_v1 import WorkerError
from ..exchange import protocol as wire

ENTRYPOINT = ["/opt/context-venv/bin/python", "-I", "-B",
              "/opt/ito/infra/aws/runner/context_exchange_entrypoint.py"]
_POLICY = {"schema","mode","account","region","cluster_arn","task_definition_arn",
           "task_role_arn","task_role_id","execution_role_arn","image","image_digest",
           "subnet_id","security_group_id","container_name","entrypoint"}


class ContextLaunchPolicy:
    def __init__(self, value):
        try:
            wire.fields(value,_POLICY)
            p = wire.detached(value)
            if (type(p["schema"]) is not int or p["schema"] != 1 or p["mode"] != "ItoDeskContextExchangeV2"
                    or p["region"] != "us-east-1" or type(p["account"]) is not str
                    or re.fullmatch(r"[0-9]{12}",p["account"]) is None):
                raise WorkerError("launch policy rejected")
            prefix="arn:aws:ecs:"+p["region"]+":"+p["account"]+":"
            patterns={
                "cluster_arn":re.escape(prefix)+r"cluster/[A-Za-z0-9_-]{1,255}",
                "task_definition_arn":re.escape(prefix)+r"task-definition/ItoDeskContextExchangeV2:[1-9][0-9]{0,8}",
                "task_role_arn":re.escape("arn:aws:iam::"+p["account"]+":role/ItoDeskContextExchangeV2"),
                "execution_role_arn":re.escape("arn:aws:iam::"+p["account"]+":role/")+r"[A-Za-z0-9_+=,.@/-]{1,512}",
                "task_role_id":r"AROA[A-Z0-9]{17}",
                "image_digest":r"sha256:[0-9a-f]{64}",
                "subnet_id":r"subnet-[0-9a-f]{17}","security_group_id":r"sg-[0-9a-f]{17}"}
            for key,pattern in patterns.items():
                if type(p[key]) is not str or re.fullmatch(pattern,p[key]) is None:
                    raise WorkerError("launch policy rejected")
            image_prefix=p["account"]+".dkr.ecr."+p["region"]+".amazonaws.com/"
            if (type(p["image"]) is not str or re.fullmatch(re.escape(image_prefix)+r"[a-z0-9._/-]+@"+re.escape(p["image_digest"]),p["image"]) is None
                    or p["container_name"] != "context-exchange-v1" or p["entrypoint"] != ENTRYPOINT):
                raise WorkerError("launch policy rejected")
            self._value=p
        except Exception:
            raise WorkerError("launch policy rejected") from None

    def request(self, coordinates):
        coords=parse_coordinates(wire.encode(coordinates));p=self._value
        return dict(cluster=p["cluster_arn"],taskDefinition=p["task_definition_arn"],
            launchType="FARGATE",platformVersion="1.4.0",count=1,
            clientToken=coords["launch_token"],startedBy=coords["launch_token"],enableExecuteCommand=False,
            networkConfiguration={"awsvpcConfiguration":{"subnets":[p["subnet_id"]],
                "securityGroups":[p["security_group_id"]],"assignPublicIp":"DISABLED"}},
            overrides={"containerOverrides":[{"name":p["container_name"],"environment":[
                {"name":"ITO_CONTEXT_BOOTSTRAP","value":wire.encode(coords).decode("ascii")}]}]},
            tags=[{"key":"canonical-sha256","value":coords["canonical_sha256"]},
                  {"key":"launch-token","value":coords["launch_token"]}])

    def observe(self, task, *, task_definition, role, network, coordinates):
        """Check trusted SDK readbacks; caller supplies provenance, never worker JSON.

        Native ENI/role observations must come from the root-owned observer. Missing
        readback is UNKNOWN/rejected, not inferred from launch-request intention.
        """
        try:
            p=self._value;expected=self.request(coordinates)
            task,definition,role,network=(wire.detached(v) for v in (task,task_definition,role,network))
            prefix=p["cluster_arn"].replace(":cluster/",":task/")+"/"
            if type(task.get("taskArn")) is not str or re.fullmatch(re.escape(prefix)+r"[0-9a-f]{32}",task["taskArn"]) is None:
                raise WorkerError("task observation rejected")
            required=dict(clusterArn=p["cluster_arn"],taskDefinitionArn=p["task_definition_arn"],
                startedBy=coordinates["launch_token"],launchType="FARGATE",platformVersion="1.4.0",lastStatus="RUNNING",enableExecuteCommand=False)
            if any(task.get(k)!=v or (k=="enableExecuteCommand" and task.get(k) is not False) for k,v in required.items()):
                raise WorkerError("task observation rejected")
            if task.get("overrides")!=expected["overrides"] or task.get("tags")!=expected["tags"]:
                raise WorkerError("task observation rejected")
            containers=task.get("containers")
            if type(containers) is not list or len(containers)!=1 or type(containers[0]) is not dict:
                raise WorkerError("task observation rejected")
            if any(containers[0].get(k)!=v for k,v in dict(name=p["container_name"],image=p["image"],imageDigest=p["image_digest"]).items()):
                raise WorkerError("task observation rejected")
            required=dict(taskDefinitionArn=p["task_definition_arn"],taskRoleArn=p["task_role_arn"],executionRoleArn=p["execution_role_arn"],
                networkMode="awsvpc",requiresCompatibilities=["FARGATE"],runtimePlatform=dict(operatingSystemFamily="LINUX",cpuArchitecture="X86_64"))
            if any(definition.get(k)!=v for k,v in required.items()):
                raise WorkerError("task observation rejected")
            defs=definition.get("containerDefinitions")
            if type(defs) is not list or len(defs)!=1 or type(defs[0]) is not dict:
                raise WorkerError("task observation rejected")
            container=defs[0]
            # Only the reviewed ephemeral scratch mount may supplement the image.
            # Canonical encoding distinguishes boolean flags from integer aliases.
            if (wire.encode(definition.get("volumes")) != wire.encode([{"name":"context-scratch"}])
                    or wire.encode(container.get("mountPoints")) != wire.encode([
                        {"sourceVolume":"context-scratch","containerPath":"/scratch","readOnly":False}])
                    or container.get("volumesFrom",[]) != []):
                raise WorkerError("task observation rejected")
            if (container.get("name")!=p["container_name"] or container.get("image")!=p["image"]
                    or container.get("entryPoint")!=ENTRYPOINT or container.get("command",[])!=[]
                    or container.get("readonlyRootFilesystem") is not True
                    or container.get("privileged",False) is not False or container.get("user")!="10001:10001"
                    or any(container.get(k,[])!=[] for k in ("environment","environmentFiles","secrets","credentialSpecs"))):
                raise WorkerError("task observation rejected")
            if role.get("Arn")!=p["task_role_arn"] or role.get("RoleId")!=p["task_role_id"]:
                raise WorkerError("task observation rejected")
            if network != dict(subnet_id=p["subnet_id"],security_group_ids=[p["security_group_id"]],public_ip=None):
                raise WorkerError("task observation rejected")
            return dict(task_arn=task["taskArn"],task_definition_arn=p["task_definition_arn"],launch_token=coordinates["launch_token"])
        except Exception:
            raise WorkerError("task observation rejected") from None


class RunTaskGuard:
    """One exact SDK invocation after root's separate canonical admission.

    An exception spends this local invocation. Durable recovery/fencing stays in
    the canonical dispatcher; replacing this object never authorizes another job.
    """
    def __init__(self, client, policy, coordinates, *, enabled=False):
        if type(policy) is not ContextLaunchPolicy or type(enabled) is not bool:
            raise WorkerError("launcher rejected")
        self._client,self._enabled=client,enabled
        self._expected=wire.encode(policy.request(coordinates))
        self._used=False
        self._lock=threading.Lock()

    def run_task(self, **request):
        with self._lock:
            if not self._enabled or self._used:
                raise WorkerError("launcher held")
            raw=wire.encode(request)
            if raw!=self._expected:
                raise WorkerError("launch request rejected")
            detached=wire.parse(raw)
            self._used=True
        try:
            return self._client.run_task(**detached)
        except Exception:
            raise WorkerError("launch outcome unknown") from None
