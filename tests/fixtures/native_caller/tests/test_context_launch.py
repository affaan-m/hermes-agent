import copy
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from infra.aws.runner import context_launch as c
spec=importlib.util.spec_from_file_location("bootstrap_fixture",Path(__file__).with_name("test_context_bootstrap.py"))
f=importlib.util.module_from_spec(spec);spec.loader.exec_module(f)

ENTRY=["/opt/context-venv/bin/python","-I","-B","/opt/ito/infra/aws/runner/context_exchange_entrypoint.py"]
POLICY=dict(schema=1,mode="ItoDeskContextExchangeV2",account="123456789012",region="us-east-1",
    cluster_arn=f.CONFIG["cluster_arn"],task_definition_arn="arn:aws:ecs:us-east-1:123456789012:task-definition/ItoDeskContextExchangeV2:7",
    task_role_arn="arn:aws:iam::123456789012:role/ItoDeskContextExchangeV2",task_role_id="AROA"+"A"*17,
    execution_role_arn="arn:aws:iam::123456789012:role/execution",image="123456789012.dkr.ecr.us-east-1.amazonaws.com/context@sha256:"+"b"*64,
    image_digest="sha256:"+"b"*64,subnet_id="subnet-"+"a"*17,security_group_id="sg-"+"b"*17,
    container_name="context-exchange-v1",entrypoint=ENTRY)

def definition():return dict(taskDefinitionArn=POLICY["task_definition_arn"],taskRoleArn=POLICY["task_role_arn"],
    executionRoleArn=POLICY["execution_role_arn"],networkMode="awsvpc",requiresCompatibilities=["FARGATE"],
    runtimePlatform=dict(operatingSystemFamily="LINUX",cpuArchitecture="X86_64"),
    volumes=[dict(name="context-scratch")],
    containerDefinitions=[dict(mountPoints=[dict(sourceVolume="context-scratch",containerPath="/scratch",readOnly=False)],name=POLICY["container_name"],image=POLICY["image"],entryPoint=ENTRY,command=[],environment=[],secrets=[],
      readonlyRootFilesystem=True,privileged=False,user="10001:10001")])

class LaunchPolicy(unittest.TestCase):
    def setUp(self):
        self.policy=c.ContextLaunchPolicy(POLICY)
        self.request=self.policy.request(f.COORDS)
        self.calls=[]
        self.sdk=types.SimpleNamespace(run_task=lambda **request:self.calls.append(request) or {"tasks":[],"failures":[]})
    def test_exact_public_override_and_private_network(self):
        env=self.request["overrides"]["containerOverrides"][0]["environment"]
        self.assertEqual(env,[{"name":"ITO_CONTEXT_BOOTSTRAP","value":f.encoded(f.COORDS).decode()}])
        self.assertEqual(self.request["count"],1)
        self.assertFalse(self.request["enableExecuteCommand"])
        self.assertEqual(self.request["networkConfiguration"]["awsvpcConfiguration"]["assignPublicIp"],"DISABLED")
    def test_extra_private_coordinate_denied(self):
        with self.assertRaises(ValueError):self.policy.request(dict(f.COORDS,input="private"))
    def test_disabled_guard_never_calls_sdk(self):
        guard=c.RunTaskGuard(self.sdk,self.policy,f.COORDS)
        with self.assertRaises(ValueError):guard.run_task(**self.request)
        self.assertEqual(self.calls,[])
    def test_success_is_one_use_and_caller_mutation_is_detached(self):
        guard=c.RunTaskGuard(self.sdk,self.policy,f.COORDS,enabled=True)
        guard.run_task(**self.request)
        self.request["count"]=9
        self.assertEqual(self.calls[0]["count"],1)
        with self.assertRaises(ValueError):guard.run_task(**self.policy.request(f.COORDS))
        self.assertEqual(len(self.calls),1)
    def test_unknown_sdk_outcome_never_retries(self):
        def fail(**request):self.calls.append(request);raise RuntimeError("synthetic private provider error")
        guard=c.RunTaskGuard(types.SimpleNamespace(run_task=fail),self.policy,f.COORDS,enabled=True)
        for _ in range(2):
            with self.assertRaises(ValueError):guard.run_task(**self.request)
        self.assertEqual(len(self.calls),1)
    def test_extra_environment_command_and_role_overrides_denied_before_sdk(self):
        for extra in ["environment","command","taskRoleArn"]:
            request=copy.deepcopy(self.request)
            if extra=="environment":request["overrides"]["containerOverrides"][0][extra].append({"name":"OTHER","value":"x"})
            elif extra=="command":request["overrides"]["containerOverrides"][0][extra]=["sh"]
            else:request["overrides"][extra]="other"
            with self.subTest(extra=extra),self.assertRaises(ValueError):c.RunTaskGuard(self.sdk,self.policy,f.COORDS,enabled=True).run_task(**request)
        self.assertEqual(self.calls,[])
    def observation(self):
        return dict(taskArn=f.TASK_ARN,clusterArn=POLICY["cluster_arn"],taskDefinitionArn=POLICY["task_definition_arn"],
            startedBy=f.COORDS["launch_token"],launchType="FARGATE",platformVersion="1.4.0",lastStatus="RUNNING",enableExecuteCommand=False,
            overrides=copy.deepcopy(self.request["overrides"]),tags=copy.deepcopy(self.request["tags"]),
            containers=[dict(name=POLICY["container_name"],image=POLICY["image"],imageDigest=POLICY["image_digest"])])
    def verify(self,task=None,taskdef=None,role=None,network=None):
        return self.policy.observe(task or self.observation(),task_definition=taskdef or definition(),
            role=role or dict(Arn=POLICY["task_role_arn"],RoleId=POLICY["task_role_id"]),
            network=network or dict(subnet_id=POLICY["subnet_id"],security_group_ids=[POLICY["security_group_id"]],public_ip=None),coordinates=f.COORDS)
    def test_exact_observation_corroborates_executor(self):
        self.assertEqual(self.verify(),dict(task_arn=f.TASK_ARN,task_definition_arn=POLICY["task_definition_arn"],launch_token=f.COORDS["launch_token"]))
    def test_wrong_task_image_or_override_denied(self):
        for kind in ["task","image","override"]:
            task=self.observation()
            if kind=="task":task["taskArn"]=task["taskArn"].replace("synthetic/","other/")
            elif kind=="image":task["containers"][0]["imageDigest"]="sha256:"+"c"*64
            else:task["overrides"]["taskRoleArn"]=POLICY["task_role_arn"]
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.verify(task=task)
    def test_recreated_role_is_not_same_identity(self):
        with self.assertRaises(ValueError):self.verify(role=dict(Arn=POLICY["task_role_arn"],RoleId="AROA"+"B"*17))
    def test_wrong_entrypoint_or_secret_task_definition_denied(self):
        for kind in ["entryPoint","secrets"]:
            value=definition();value["containerDefinitions"][0][kind]=["different"]
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.verify(taskdef=value)
    def test_unobserved_network_or_public_ip_denied(self):
        for value in [dict(subnet_id=POLICY["subnet_id"],security_group_ids=[],public_ip=None),dict(subnet_id=POLICY["subnet_id"],security_group_ids=[POLICY["security_group_id"]],public_ip="192.0.2.1")]:
            with self.subTest(value=value),self.assertRaises(ValueError):self.verify(network=value)

if __name__=="__main__":unittest.main(verbosity=2)
