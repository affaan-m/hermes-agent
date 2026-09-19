"""Protected host adapters. Importing this module has no SDK or provider effects.

Factories require root-supplied credentials/authority, never model arguments.
MQTT delivery provenance is an owner-local opaque capability minted only by the
actual TLS client's callback. Canonical approval/acceptance are never invented.
"""
from __future__ import annotations
import base64
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
import time

VERIFIED_TOPIC = 'ito-context/v2/verified'
CLIENT_ID = 'ito-context-mini-v2'
SDK_PINS = {'awsiotsdk':'1.31.0','awscrt':'0.36.1'}

class HostError(Exception):
    """Only fixed public reason codes; no provider exception text."""


def encoded(value):
    try:return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode('ascii')
    except Exception:raise HostError('invalid_document') from None


def _pairs(items):
    d={}
    for k,v in items:
        if k in d:raise HostError('duplicate_field')
        d[k]=v
    return d


def document(raw,limit=90*1024):
    try:
        if type(raw) is not bytes or not 0<len(raw)<=limit:raise HostError('document_size')
        d=json.loads(raw,object_pairs_hook=_pairs,parse_constant=lambda _: (_ for _ in ()).throw(HostError('nonfinite')))
        if type(d) is not dict:raise HostError('document_type')
        encoded(d)
        return d
    except Exception:raise HostError('invalid_document') from None


def number(x):
    if type(x) not in (int,float):raise HostError('invalid_time')
    try:
        if not math.isfinite(x) or x<0:raise HostError('invalid_time')
        return float(x)
    except (ValueError,OverflowError):raise HostError('invalid_time') from None


def digest(raw):return hashlib.sha256(raw).hexdigest()

class Budget:
    """Physical owner and nonrenewable wall/monotonic deadline."""
    def __init__(self,deadline,clock=time.time,monotonic=time.monotonic):
        self.owner=threading.current_thread();self.clock=clock;self.mono=monotonic
        self.last=(number(clock()),number(monotonic()));self.deadline=number(deadline)
        self.end=self.last[1]+max(0,self.deadline-self.last[0]);self.failed=False;self.effective=self.last[0]
    def remaining(self,deadline=None):
        if threading.current_thread() is not self.owner or self.failed:raise HostError('owner_or_clock')
        now=(number(self.clock()),number(self.mono()))
        if any(a<b for a,b in zip(now,self.last)):
            self.failed=True;raise HostError('clock_rollback')
        self.effective=max(now[0],self.effective+now[1]-self.last[1])
        self.last=now
        remaining=min(self.deadline-self.effective,self.end-now[1])
        if deadline is not None:remaining=min(remaining,number(deadline)-self.effective)
        if remaining<=0:raise HostError('deadline')
        return remaining


def installed_mqtt():
    try:
        if any(importlib.metadata.version(k)!=v for k,v in SDK_PINS.items()):raise HostError('sdk_version')
        from awsiot import mqtt5_client_builder
        from awscrt import mqtt5
        return mqtt5_client_builder.mtls_from_path,mqtt5
    except Exception:raise HostError('mqtt_dependency_unavailable') from None

class _Delivery:
    __slots__=()

class X509Transport:
    """Per-attempt clean-session MQTT5 transport, no reconnect authority/replay.

    Root must deploy the exact IoT rule/policies: only its rule role publishes
    VERIFIED_TOPIC. TLS plus that broker policy is required provenance. Worker
    JSON alone is never a verified delivery. Direct callback access is trusted
    host code; these objects must never enter a tool result.
    """
    def __init__(self,*,endpoint,cert_path,key_path,ca_path,role_id,coordinates,
                 offer,deadline_at,clock=time.time,monotonic=time.monotonic,
                 builder=None,mqtt5=None,startup_deadline_at=None):
        if (type(endpoint) is not str or re.fullmatch(r'[a-zA-Z0-9-]+-ats\.iot\.us-east-1\.amazonaws\.com',endpoint) is None
                or type(role_id) is not str or re.fullmatch(r'AROA[A-Z0-9]{17}',role_id) is None):raise HostError('mqtt_configuration')
        if any(type(x) is not str or not Path(x).is_absolute() for x in (cert_path,key_path,ca_path)):raise HostError('tls_paths')
        for key in ('attempt_id','request_id','launch_token'):
            if type(coordinates.get(key)) is not str or re.fullmatch('[0-9a-f]{32}',coordinates[key]) is None:raise HostError('coordinates')
        self.coords=json.loads(encoded(coordinates));self.role=role_id;self.offer=offer
        self.budget=Budget(deadline_at,clock,monotonic);self.clock=clock
        self._lock=threading.Lock();self._pending={};self._verified={};self._fault=False
        self._closed=False;self._started=False;self._stop_called=False;self._ever_connected=False
        self._connected=threading.Event();self._stopped=threading.Event();self._client=None
        self._principal=None;self._reply_expiry=0
        if builder is None or mqtt5 is None:
            if builder is not None or mqtt5 is not None:raise HostError('sdk_injection_pair')
            builder,mqtt5=installed_mqtt()
        self.mqtt=mqtt5
        try:
            remaining=self.budget.remaining(startup_deadline_at)
            self._client=builder(cert_filepath=cert_path,pri_key_filepath=key_path,ca_filepath=ca_path,
                endpoint=endpoint,port=8883,client_id=CLIENT_ID,enable_metrics_collection=False,
                session_behavior=mqtt5.ClientSessionBehaviorType.CLEAN,session_expiry_interval_sec=0,
                offline_queue_behavior=mqtt5.ClientOperationQueueBehaviorType.FAIL_ALL_ON_DISCONNECT,
                maximum_packet_size=90*1024,connack_timeout_ms=max(1,int(min(remaining,5)*1000)),
                ack_timeout_sec=1,min_reconnect_delay_ms=60000,max_reconnect_delay_ms=60000,
                on_lifecycle_connection_success=self._success,on_lifecycle_connection_failure=self._failure,
                on_lifecycle_disconnection=self._failure,on_lifecycle_stopped=self._stop_event,
                on_publish_received=self._publication)
            self._started=True;self._client.start()
            if not self._connected.wait(self.budget.remaining(startup_deadline_at)):raise HostError('connect_unknown')
            self._check()
            packet=mqtt5.SubscribePacket(subscriptions=[mqtt5.Subscription(topic_filter=VERIFIED_TOPIC,qos=mqtt5.QoS.AT_MOST_ONCE,
                retain_handling_type=mqtt5.RetainHandlingType.DONT_SEND)])
            ack=self._client.subscribe(packet).result(timeout=self.budget.remaining(startup_deadline_at))
            self._check()
            if len(ack.reason_codes)!=1 or int(ack.reason_codes[0])!=0:raise HostError('subscribe_rejected')
        except Exception:
            self.close();raise HostError('transport_open_failed') from None
    def _success(self,data):
        try:
            with self._lock:
                if self._ever_connected or self._closed or int(data.connack_packet.reason_code)!=0 or data.connack_packet.session_present is not False:self._fault=True
                self._ever_connected=True;self._connected.set()
        except Exception:
            self._failure(None)
    def _failure(self,_data):
        with self._lock:self._fault=True;self._pending.clear();self._connected.set()
    def _stop_event(self,_data):self._stopped.set()
    def _publication(self,data):
        try:
            packet=data.publish_packet;raw=packet.payload
            with self._lock:
                if self._closed or self._fault:return
                if (packet.topic!=VERIFIED_TOPIC or packet.retain is not False or int(packet.qos)!=0
                        or type(raw) is not bytes or not 0<len(raw)<=90*1024 or len(self._pending)>=4):
                    self._fault=True;self._pending.clear();return
                token=os.urandom(32);self._pending[token]=raw
            if self.offer(token) is not True:
                with self._lock:self._pending.pop(token,None)
        except Exception:
            with self._lock:self._fault=True;self._pending.clear()
    def _check(self,deadline=None):
        self.budget.remaining(deadline)
        with self._lock:
            if self._fault or self._closed or not self._ever_connected:raise HostError('transport_held')
    def poll(self,*,deadline_at):self._check(deadline_at)
    def decode_delivery(self,token):
        self._check()
        with self._lock:raw=self._pending.pop(token,None) if type(token) is bytes else None
        if raw is None:raise HostError('unverified_delivery')
        d=document(raw)
        if set(d)!={'envelope_schema','principal','client_id','topic','received_at_ms','body_b64'}:raise HostError('broker_fields')
        principal=d['principal']
        if (d['envelope_schema']!='ito-context-broker/2' or type(principal) is not str
                or re.fullmatch(re.escape(self.role)+':[0-9a-f]{32}',principal) is None
                or d['client_id']!=principal or d['topic']!='ito-context/v2/request/'+principal):raise HostError('broker_identity')
        if type(d['received_at_ms']) is not int:raise HostError('broker_time')
        at=number(d['received_at_ms'])/1000
        if not 0<=self.budget.effective-at<2:raise HostError('broker_expired')
        try:
            b64=d['body_b64']
            if type(b64) is not str or not 0<len(b64)<=87384:raise HostError('body_size')
            rawbody=base64.b64decode(b64,validate=True)
            if base64.b64encode(rawbody).decode()!=b64:raise HostError('body_encoding')
            body=document(rawbody,65536)
        except Exception:raise HostError('broker_body') from None
        if any(body.get(k)!=self.coords[k] for k in ('attempt_id','request_id')):raise HostError('broker_request')
        if self._principal is not None and principal!=self._principal:raise HostError('broker_executor_changed')
        handle=_Delivery()
        if len(self._verified)>=4:raise HostError('delivery_budget')
        self._verified[handle]=dict(principal=principal,client_id=principal,topic=d['topic'],received_at=at,body=rawbody)
        return handle
    def verify_broker(self,handle):
        self._check();d=self._verified.pop(handle,None)
        if d is None or not 0<=self.budget.effective-d['received_at']<2:raise HostError('unverified_delivery')
        self._principal=d['principal'];self._reply_expiry=d['received_at']+2
        return d
    def send(self,topic,data):
        self._check(self._reply_expiry)
        if self._principal is None or topic!='ito-context/v2/reply/'+self._principal or type(data) is not bytes or not 0<len(data)<=65536:raise HostError('reply_binding')
        try:
            packet=self.mqtt.PublishPacket(topic=topic,payload=data,qos=self.mqtt.QoS.AT_MOST_ONCE,retain=False)
            self._client.publish(packet).result(timeout=self.budget.remaining(self._reply_expiry))
            self._check(self._reply_expiry)
        except Exception:
            self._fault=True;raise HostError('send_unknown') from None
        return True
    def close(self):
        if threading.current_thread() is not self.budget.owner:return False
        with self._lock:
            self._closed=True;self._pending.clear();self._verified.clear()
        if self._client is None or not self._started:return True
        if not self._stop_called:
            self._stop_called=True
            try:self._client.stop()
            except Exception:return False
        # Cleanup has its own bounded wait, never grants execution or reconnect.
        return self._stopped.wait(1.0)

class ExistingApproval:
    """Consumes an authenticated external approval already in the canonical DB.

    resolve is a root-owned authority port, not a document passed by the model.
    It resolves the exact requested hashes using actual independent identity.
    No method here calls canonical.approve or canonical.accept.
    """
    def __init__(self,*,resolve,clock=time.time,monotonic=time.monotonic):
        self.resolve=resolve;self.clock=clock;self.mono=monotonic;self._used=False;self.envelope=None
    def __call__(self,owner,typed,spec,*,deadline_at):
        b=Budget(deadline_at,self.clock,self.mono);b.remaining()
        if self._used:raise HostError('approval_spent')
        self._used=True
        try:
            td=document(typed,8192);document(spec,8192)
            v=self.resolve(spec_sha256=digest(typed),request_spec_sha256=digest(spec),deadline_at=deadline_at)
            b.remaining()
            if type(v) is not dict or set(v)!={'principal','kind','expires_at','request_spec_sha256','envelope'}:raise HostError('approval_missing')
            env=json.loads(encoded(v['envelope']))
            if (v['kind']!='reviewer' or v['principal'] not in owner.canonical.reviewers
                    or v['principal'] in (env['dispatcher_id'],env['claim_owner'])
                    or number(v['expires_at'])<=b.effective
                    or v['request_spec_sha256']!=digest(spec) or env['spec_sha256']!=digest(typed)
                    or env['task_id']!=td['task_id']):raise HostError('approval_binding')
            attempt=owner.canonical._attempt(env)
            if attempt['state']!='approved_effects_unknown' or attempt['admitted']!=0:raise HostError('approval_state')
            b.remaining();self.envelope=env;return json.loads(encoded(env))
        except Exception:raise HostError('approval_unavailable') from None

class BotoCalls:
    """Bounded clients from an explicit credential-bearing session supplied by root.

    Never constructs a default session or searches credentials. Root must supply
    an already authenticated session with nonblocking credentials; expiration
    holds. One request, no SDK retry or waiter; pending ECS states return to the
    original owner for bounded polling. No provider call occurs at construction.
    """
    def __init__(self,*,session,clock=time.time,monotonic=time.monotonic,config_factory=None):
        self.session=session;self.clock=clock;self.mono=monotonic;self.config_factory=config_factory
        self.owner=threading.current_thread();self.calls=0
    def __call__(self,service,operation,*,deadline_at,**kwargs):
        b=Budget(deadline_at,self.clock,self.mono)
        if threading.current_thread() is not self.owner or service not in ('ecs','iam','ec2') or self.calls>=256:raise HostError('provider_budget')
        allowed={'ecs':{'run_task','describe_tasks','describe_task_definition'},'iam':{'get_role'},'ec2':{'describe_network_interfaces'}}
        if operation not in allowed[service]:raise HostError('provider_operation')
        client=None
        try:
            self.calls+=1;remaining=b.remaining();config_factory=self.config_factory
            if config_factory is None:
                from botocore.config import Config
                config_factory=Config
            config=config_factory(connect_timeout=min(2,remaining/3),read_timeout=min(2,remaining/3),
                retries={'total_max_attempts':1,'mode':'standard'},proxies={})
            client=self.session.client(service,region_name='us-east-1',config=config)
            b.remaining();result=getattr(client,operation)(**kwargs);b.remaining()
            if type(result) is not dict:raise HostError('provider_shape')
            return result
        except Exception:raise HostError('provider_outcome_unknown') from None
        finally:
            if client is not None:
                try:client.close()
                except Exception:raise HostError('provider_close_unknown') from None

class ECSObserver:
    """Fresh ECS/IAM/ENI observations, projected into the accepted V2 policy.

    Extra provider timestamps are not serialized into the exact policy adapter.
    Security-relevant fields come from API responses, never the requested config.
    """
    TASK_FIELDS=('taskArn','clusterArn','taskDefinitionArn','startedBy','launchType','platformVersion',
                 'lastStatus','enableExecuteCommand','overrides','tags','containers')
    DEF_FIELDS=('taskDefinitionArn','taskRoleArn','executionRoleArn','networkMode','requiresCompatibilities',
                'runtimePlatform','volumes','containerDefinitions')
    CONTAINER_FIELDS=('name','image','entryPoint','command','environment','secrets','environmentFiles',
                      'credentialSpecs','readonlyRootFilesystem','privileged','user','mountPoints','volumesFrom')
    def __init__(self,*,call,policy,coordinates,deadline_at,clock=time.time,monotonic=time.monotonic):
        self.call=call;self.policy=policy;self.coordinates=json.loads(encoded(coordinates))
        self.budget=Budget(deadline_at,clock,monotonic);self.clock=clock;self.executor=None
    def _read(self,service,operation,deadline,**kwargs):
        self.budget.remaining(deadline);r=self.call(service,operation,deadline_at=min(deadline,self.budget.deadline),**kwargs);self.budget.remaining(deadline)
        if type(r) is not dict or r.get('failures',[])!=[] or r.get('nextToken') or r.get('NextToken'):raise HostError('provider_incomplete')
        return r
    @staticmethod
    def _one(rows):
        if type(rows) is not list or len(rows)!=1 or type(rows[0]) is not dict:raise HostError('provider_cardinality')
        return rows[0]
    def _observe(self,task_arn,deadline,*,allow_stopped=False):
        p=self.policy._value
        task=self._one(self._read('ecs','describe_tasks',deadline,cluster=p['cluster_arn'],tasks=[task_arn],include=['TAGS'])['tasks'])
        if task.get('taskArn')!=task_arn:raise HostError('task_identity')
        status=task.get('lastStatus')
        if status!='RUNNING' and not (allow_stopped and status=='STOPPED'):raise HostError('task_not_ready')
        definition=self._read('ecs','describe_task_definition',deadline,taskDefinition=p['task_definition_arn'])['taskDefinition']
        role=self._read('iam','get_role',deadline,RoleName=p['task_role_arn'].rsplit('/',1)[1])['Role']
        attachments=[a for a in task.get('attachments',[]) if a.get('type')=='ElasticNetworkInterface']
        attachment=self._one(attachments)
        details=attachment.get('details',[])
        ids=[v['value'] for v in details if v.get('name')=='networkInterfaceId']
        if len(ids)!=1 or type(ids[0]) is not str or re.fullmatch(r'eni-[0-9a-f]{17}',ids[0]) is None:raise HostError('eni_binding')
        eni=self._one(self._read('ec2','describe_network_interfaces',deadline,NetworkInterfaceIds=ids)['NetworkInterfaces'])
        if eni.get('NetworkInterfaceId')!=ids[0] or eni.get('OwnerId')!=p['account']:raise HostError('eni_identity')
        groups=eni.get('Groups')
        if type(groups) is not list or any(type(g) is not dict or type(g.get('GroupId')) is not str for g in groups):raise HostError('eni_groups')
        association=eni.get('Association',{})
        if type(association) is not dict:raise HostError('eni_association')
        network=dict(subnet_id=eni.get('SubnetId'),security_group_ids=[g['GroupId'] for g in groups],public_ip=association.get('PublicIp'))
        view={k:task[k] for k in self.TASK_FIELDS}
        view['containers']=[{k:c[k] for k in ('name','image','imageDigest')} for c in task['containers']]
        # API tag list order is unspecified; preserve all values and reject duplicates.
        tags=task['tags'];keys=[t['key'] for t in tags]
        if len(keys)!=len(set(keys)) or set(keys)!={'canonical-sha256','launch-token'}:raise HostError('task_tags')
        tagmap={t['key']:t['value'] for t in tags};view['tags']=[{'key':k,'value':tagmap[k]} for k in ('canonical-sha256','launch-token')]
        dv={k:definition[k] for k in self.DEF_FIELDS}
        dv['containerDefinitions']=[{k:c[k] for k in self.CONTAINER_FIELDS if k in c} for c in definition['containerDefinitions']]
        roleview={k:role[k] for k in ('Arn','RoleId')}
        if status=='STOPPED':
            # The accepted guard checks startup RUNNING only. Revalidate the
            # same immutable/security fields in a detached view, then separately
            # report the actual STOPPED status. This does not assert RUNNING.
            view['lastStatus']='RUNNING'
        executor=self.policy.observe(view,task_definition=dv,role=roleview,network=network,coordinates=self.coordinates)
        if self.executor is not None and executor!=self.executor:raise HostError('executor_changed')
        return dict(task=view,task_definition=dv,role=roleview,network=network),executor,status,task
    def observe_launch(self,response,coordinates,*,deadline_at):
        try:
            if coordinates!=self.coordinates or type(response) is not dict or response.get('failures',[])!=[]:raise HostError('launch_response')
            task=self._one(response['tasks']);observed,executor,status,raw=self._observe(task['taskArn'],deadline_at)
            self.executor=executor;return observed
        except Exception:raise HostError('launch_observation_unavailable') from None
    def observe_executor(self,expected):
        try:
            if self.executor is None or expected!=self.executor:raise HostError('executor_binding')
            _,executor,status,_=self._observe(expected['task_arn'],self.budget.deadline,allow_stopped=True)
            p=self.policy._value
            return dict(executor=executor,role_id=p['task_role_id'],task_role_arn=p['task_role_arn'],
                        image_sha256=p['image_digest'][7:],canonical_sha256=self.coordinates['canonical_sha256'],status=status)
        except Exception:raise HostError('executor_observation_unavailable') from None
    def terminal(self,expected,*,deadline_at):
        if expected!=self.executor or expected is None:raise HostError('terminal_binding')
        try:
            _,executor,status,raw=self._observe(expected['task_arn'],deadline_at,allow_stopped=True)
            if status!='STOPPED':return None
            container=self._one(raw['containers'])
            if type(container.get('exitCode')) is not int or container['exitCode']!=0 or raw.get('stopCode')!='EssentialContainerExited':raise HostError('terminal_failed')
            return {'executor':executor,'exit_code':container['exitCode']}
        except Exception:raise HostError('terminal_observation_unavailable') from None

class TerminalReceipts:
    """Imports existing externally authored claim/start/terminal bytes only.

    read_receipt is a root-owned authenticated, bounded receipt read port, not
    worker MQTT fields. This adapter never synthesizes those durable receipts.
    A stopped ECS task corroborates identity/exit, not capture or cleanup.
    """
    def __init__(self,*,approval,observer,read_receipt,clock=time.time,monotonic=time.monotonic):
        self.approval=approval;self.observer=observer;self.read=read_receipt;self.clock=clock;self.mono=monotonic;self._done=None;self._binding=None;self._owner=threading.current_thread();self._budget=None
    def corroborate_terminal(self,owner,exchange,binding,*,deadline_at):
        if threading.current_thread() is not self._owner:raise HostError('terminal_owner')
        if self._budget is None:self._budget=Budget(deadline_at,self.clock,self.mono)
        b=self._budget;b.remaining(deadline_at)
        key=encoded(binding)
        if self._binding is not None and self._binding!=key:raise HostError('terminal_binding')
        self._binding=key
        if self._done is not None:return json.loads(encoded(self._done))
        env=self.approval.envelope
        if env is None or digest(encoded(env))!=binding['canonical_sha256']:raise HostError('terminal_envelope')
        terminal=self.observer.terminal(binding['executor'],deadline_at=deadline_at)
        if terminal is None:return None
        try:
            evidence=exchange.read_process_summary(binding)
            cache={}
            def read(key):
                b.remaining()
                if key not in cache:
                    raw=self.read(key,deadline_at=deadline_at)
                    if raw is not None:document(raw,16384)
                    cache[key]=raw
                b.remaining();return cache[key]
            inspected=owner.canonical.inspect_receipts(env,read)
            if inspected['state']!='execution_succeeded_pending_review' or encoded(inspected['result'])!=encoded(evidence['summary']):raise HostError('terminal_receipt_mismatch')
            if inspected['result']['exit_code']!=terminal['exit_code']:raise HostError('terminal_exit')
            result=owner.canonical.import_receipts(env,read);b.remaining()
            if result['state']!='execution_succeeded_pending_review' or result['retryable'] is not False:raise HostError('terminal_import')
            self._done=dict(schema=2,canonical_sha256=binding['canonical_sha256'],spec_sha256=binding['spec_sha256'],result=inspected['result'],acceptance_verified=False)
            return json.loads(encoded(self._done))
        except Exception:raise HostError('terminal_receipts_unavailable') from None
    def reconcile(self,owner,env,executor,*,deadline_at):
        if threading.current_thread() is not self._owner:raise HostError('terminal_owner')
        if self._budget is not None:self._budget.remaining(deadline_at)
        else:Budget(deadline_at,self.clock,self.mono).remaining()
        if self._done is None or self.approval.envelope!=env or self.observer.executor!=executor:return {'resolved':False}
        attempt=owner.canonical._attempt(env)
        return {'resolved':attempt['state']=='execution_succeeded_pending_review' and attempt['admitted']==1}

class ArtifactCleanup:
    """Remove only exact per-binding files in the pinned locked ArtifactStore.

    Uses its existing checked directory fd, binding/header and payload validation.
    No recursive deletion, selected database mutation, retention-clock change or
    claim that backups/logs were erased. Reconciliation receipts remain outside
    this artifact directory, under root's independent custody.
    """
    def __init__(self,*,clock=time.time,monotonic=time.monotonic):self.clock=clock;self.mono=monotonic
    def __call__(self,owner,transport,binding,*,deadline_at):
        a=owner.artifacts;clean=False
        try:
            if deadline_at is None:raise HostError('cleanup_deadline')
            b=Budget(deadline_at,self.clock,self.mono);b.remaining()
            if transport is not None and (not transport._closed or not transport._stopped.is_set()):raise HostError('transport_not_stopped')
            with a._mutex:
                a._check();names,_=a._names();selected=[]
                if binding is not None:
                    for kind in ('input','result','summary'):
                        name,_=a._key(binding,kind)
                        if name in names:
                            a._load(name);entry=os.stat(name,dir_fd=a._fd,follow_symlinks=False)
                            selected.append((name,entry))
                for name,before in selected:
                    b.remaining();a._check();after=os.stat(name,dir_fd=a._fd,follow_symlinks=False)
                    if (before.st_dev,before.st_ino,before.st_mtime_ns,before.st_ctime_ns,before.st_size)!=(after.st_dev,after.st_ino,after.st_mtime_ns,after.st_ctime_ns,after.st_size):raise HostError('artifact_changed')
                    os.unlink(name,dir_fd=a._fd)
                os.fsync(a._fd);remaining,_=a._names()
                if any(name in remaining for name,_ in selected):raise HostError('artifact_remains')
                b.remaining();clean=True
        except Exception:clean=False
        finally:
            try:a.close()
            except Exception:clean=False
        return clean


class HostBundle:
    """One-attempt concrete DispatchPorts assembly using exact supplied modules.

    modules holds the reviewed native dispatcher, ContextLaunchPolicy/RunTaskGuard,
    ActivationRouter/ExchangeController and ContextResults classes. Root checks
    their installed/loaded origins. create_owner returns the existing single
    authenticated native/canonical graph; never recreated inside an SDK callback.
    """
    def __init__(self,*,modules,create_owner,policy,provider_call,mqtt_config,
                 approval_resolver,receipt_reader,request_deadline,clock=time.time,
                 monotonic=time.monotonic,mqtt_builder=None,mqtt5=None):
        self.modules=modules;self.create_owner=create_owner;self.policy=policy;self.call=provider_call
        self.config=dict(mqtt_config);self.deadline=number(request_deadline)
        self.clock=clock;self.mono=monotonic;self.budget=Budget(self.deadline+60,clock,monotonic)
        self.approval=ExistingApproval(resolve=approval_resolver,clock=clock,monotonic=monotonic)
        self.receipt_reader=receipt_reader;self.transport=None;self.observer=None;self.terminal_port=None
        self.mqtt_builder=mqtt_builder;self.mqtt5=mqtt5;self._opened=False
    def open_transport(self,offer,coordinates,*,deadline_at):
        self.budget.remaining(deadline_at)
        if self._opened:raise HostError('transport_already_spent')
        self._opened=True
        self.observer=ECSObserver(call=self.call,policy=self.policy,coordinates=coordinates,deadline_at=self.deadline+60,clock=self.clock,monotonic=self.mono)
        self.terminal_port=TerminalReceipts(approval=self.approval,observer=self.observer,read_receipt=self.receipt_reader,clock=self.clock,monotonic=self.mono)
        self.transport=X509Transport(**self.config,role_id=self.policy._value['task_role_id'],coordinates=coordinates,
            offer=offer,deadline_at=self.deadline+60,startup_deadline_at=deadline_at,
            clock=self.clock,monotonic=self.mono,builder=self.mqtt_builder,mqtt5=self.mqtt5)
        return self.transport
    def make_launch_guard(self,coordinates):
        self.budget.remaining(self.deadline)
        if self.observer is None or self.observer.coordinates!=coordinates:raise HostError('launch_coordinates')
        deadline=min(self.deadline,number(self.clock())+30)
        bundle=self
        class RunClient:
            def run_task(self,**kwargs):return bundle.call('ecs','run_task',deadline_at=deadline,**kwargs)
        return self.modules.RunTaskGuard(RunClient(),self.policy,coordinates,enabled=True)
    def observe_launch(self,*args,**kwargs):return self.observer.observe_launch(*args,**kwargs)
    def observe_executor(self,*args,**kwargs):return self.observer.observe_executor(*args,**kwargs)
    def verify_broker(self,handle):
        if self.transport is None:raise HostError('transport_missing')
        return self.transport.verify_broker(handle)
    def corroborate_terminal(self,*args,**kwargs):return self.terminal_port.corroborate_terminal(*args,**kwargs)
    def reconcile(self,*args,**kwargs):
        if self.terminal_port is None:return {'resolved':False}
        return self.terminal_port.reconcile(*args,**kwargs)
    def ports(self):
        self.budget.remaining()
        return self.modules.DispatchPorts(create_owner=self.create_owner,approve=self.approval,
            launch_policy=self.policy,make_launch_guard=self.make_launch_guard,observe_launch=self.observe_launch,
            router_factory=self.modules.ActivationRouter,exchange_factory=self.modules.ExchangeController,
            results_factory=self.modules.ContextResults,verify_broker=self.verify_broker,
            observe_executor=self.observe_executor,open_transport=self.open_transport,
            corroborate_terminal=self.corroborate_terminal,reconcile=self.reconcile,
            cleanup_resources=ArtifactCleanup(clock=self.clock,monotonic=self.mono))


def prepared_factory(*,prepared_class,dispatcher_class,resolve_request,make_bundle,
                     validate_request,project_context,clock=time.time,monotonic=time.monotonic):
    """Factory(agent=...) seam for the frozen gateway PreparedContextCall.

    resolve_request(agent) is trusted enrollment lookup; it returns host/controller
    opaque handles and exact dispatcher.run kwargs. validate_request is a fresh
    authoritative lookup (including original agent/message scope), not a flag in
    model arguments. project_context performs explicit audience-safe projection.
    No Store/provider work occurs until invoke; no default cloud/local fallback.
    """
    def factory(*,agent):
        request=resolve_request(agent)
        if type(request) is not dict or set(request)!={'host_caller','controller_caller','run_kwargs','expires_at'}:raise HostError('request_unavailable')
        run_kwargs=request['run_kwargs']
        if type(run_kwargs) is not dict:raise HostError('request_shape')
        budget=Budget(request['expires_at'],clock,monotonic);used=False;result=None
        def validate():
            try:budget.remaining();return validate_request(agent,request) is True
            except Exception:return False
        def invoke():
            nonlocal used,result
            if used or not validate():raise HostError('request_spent_or_revoked')
            used=True
            bundle=make_bundle(request)
            dispatcher=dispatcher_class(ports=bundle.ports(),host_caller=request['host_caller'],controller_caller=request['controller_caller'],enabled=True,clock=clock,monotonic_clock=monotonic)
            result=dispatcher.run(**run_kwargs)
            return result
        def project(outcome):
            if outcome is not result or result is None or not validate() or outcome.state!='completed':raise HostError('result_unavailable')
            receipt=outcome.closure_receipt
            if (type(receipt) is not dict or receipt.get('cleanup_complete') is not True
                    or receipt.get('durable_capacity_released') is not False):raise HostError('closure_unavailable')
            # Explicit host policy returns only conversation-approved context text.
            text=project_context(agent,request,outcome.context_result)
            if type(text) is not str or not text or len(encoded({'status':'completed','context':text}))>8192:raise HostError('public_projection')
            return {'status':'completed','context':text}
        if not validate():raise HostError('request_unavailable')
        return prepared_class(invoke=invoke,validate=validate,project_result=project,
                              expires_at=request['expires_at'],clock=clock,monotonic_clock=monotonic)
    return factory
