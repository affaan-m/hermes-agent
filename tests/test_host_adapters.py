"""Focused tests for the reviewed host adapters (gateway/host_adapters.py).

Ported from the private 2026-09-14 packet (original pin 25c6fc86): the test
bodies are byte-identical; only module/fixture resolution changed. The module
under test is loaded from this repo; the pinned ito-desk worker/exchange
inputs resolve from the vendored, hash-verified copies under
tests/fixtures/native_caller (see PINS.json there).
"""
import base64
import copy
import hashlib
import importlib.util
import json
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace as NS

ROOT = Path(__file__).resolve().parents[1]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


H = _load('host_adapters_under_test', ROOT / 'gateway' / 'host_adapters.py')
ROLE = 'AROA' + 'A'*17
TASK = 'b'*32
PRINCIPAL = ROLE + ':' + TASK
COORDS = dict(attempt_id='a'*32, request_id='c'*32, launch_token='d'*32,
              request_spec_sha256='e'*64,spec_sha256='f'*64,input_sha256='1'*64,
              source_sha256='2'*64,canonical_sha256='3'*64)
class Clock:
    def __init__(self): self.wall=1000.; self.mono=10.
    def time(self): return self.wall
    def monotonic(self): return self.mono
    def advance(self,n):self.wall+=n;self.mono+=n
class Future:
    def __init__(self,value=None,error=None):self.value=value;self.error=error;self.timeouts=[]
    def result(self,timeout):
        self.timeouts.append(timeout)
        if self.error:raise self.error
        return self.value
class Packet(NS): pass
class SDK:
    QoS=NS(AT_MOST_ONCE=0)
    ClientSessionBehaviorType=NS(CLEAN=0)
    ClientOperationQueueBehaviorType=NS(FAIL_ALL_ON_DISCONNECT=3)
    RetainHandlingType=NS(DONT_SEND=2)
    SubscribePacket=Subscription=PublishPacket=Packet
class Client:
    def __init__(self,**kw):self.kw=kw;self.sent=[];self.stops=0;self.subs=[]
    def start(self):self.kw['on_lifecycle_connection_success'](NS(connack_packet=NS(reason_code=0,session_present=False)))
    def subscribe(self,p):self.subs.append(p);return Future(NS(reason_codes=[0]))
    def publish(self,p):self.sent.append(p);return Future(NS())
    def stop(self):self.stops+=1;self.kw['on_lifecycle_stopped'](NS())
    def emit(self,raw,**kw):self.kw['on_publish_received'](NS(publish_packet=Packet(topic=kw.get('topic',H.VERIFIED_TOPIC),payload=raw,qos=0,retain=kw.get('retain',False))))
class TransportTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.tokens=[];self.clients=[]
        def build(**kw):c=Client(**kw);self.clients.append(c);return c
        self.transport=H.X509Transport(endpoint='example-ats.iot.us-east-1.amazonaws.com',cert_path='/synthetic/cert',key_path='/synthetic/key',ca_path='/synthetic/ca',role_id=ROLE,coordinates=COORDS,offer=lambda b:self.tokens.append(b) or True,deadline_at=1030,clock=self.clock.time,monotonic=self.clock.monotonic,builder=build,mqtt5=SDK)
        self.client=self.clients[0]
    def tearDown(self):self.transport.close()
    def envelope(self,**overrides):
        body=json.dumps(dict(COORDS,verb='BOOTSTRAP')).encode()
        d=dict(envelope_schema='ito-context-broker/2',principal=PRINCIPAL,client_id=PRINCIPAL,topic='ito-context/v2/request/'+PRINCIPAL,received_at_ms=1000000,body_b64=base64.b64encode(body).decode());d.update(overrides);return json.dumps(d).encode()
    def receive(self,raw):self.client.emit(raw);return self.transport.decode_delivery(self.tokens.pop(0))
    def test_real_factory_arguments_clean_tls_and_exact_subscription(self):
        self.assertEqual(self.client.kw['port'],8883);self.assertEqual(self.client.kw['client_id'],'ito-context-mini-v2')
        self.assertEqual(self.client.kw['session_expiry_interval_sec'],0);self.assertEqual(self.client.kw['offline_queue_behavior'],3)
        self.assertEqual(self.client.kw['cert_filepath'],'/synthetic/cert');self.assertEqual(self.client.subs[0].subscriptions[0].topic_filter,H.VERIFIED_TOPIC)
    def test_callback_only_offers_opaque_token_then_owner_verifies(self):
        raw=self.envelope();handle=self.receive(raw);self.assertEqual(self.transport.verify_broker(handle)['principal'],PRINCIPAL)
        with self.assertRaises(H.HostError):self.transport.verify_broker(handle)
    def test_forged_raw_cannot_enter_verified_channel(self):
        with self.assertRaises(H.HostError):self.transport.decode_delivery(self.envelope())
    def test_strict_broker_identity_and_time(self):
        for patch in [dict(principal='human'),dict(client_id='wrong'),dict(topic='ito-context/v2/request/other'),dict(received_at_ms=True),dict(received_at_ms=998000),dict(received_at_ms=1000001),dict(envelope_schema='other'),dict(body_b64='!!!!')]:
            with self.subTest(patch=list(patch)):
                with self.assertRaises(H.HostError):self.receive(self.envelope(**patch))
    def test_duplicate_json_field_rejected(self):
        raw=self.envelope()[:-1]+b',"principal":"human"}'
        with self.assertRaises(H.HostError):self.receive(raw)
    def test_cross_request_body_rejected(self):
        body=json.dumps(dict(COORDS,request_id='9'*32)).encode()
        with self.assertRaises(H.HostError):self.receive(self.envelope(body_b64=base64.b64encode(body).decode()))
    def test_retained_or_wrong_topic_is_permanent_fault(self):
        self.client.emit(self.envelope(),retain=True)
        self.assertFalse(self.tokens)
        with self.assertRaises(H.HostError):self.transport.poll(deadline_at=1001)
    def test_callback_capacity_bounded(self):
        for _ in range(5):self.client.emit(self.envelope())
        self.assertLessEqual(len(self.tokens),4)
        with self.assertRaises(H.HostError):self.transport.poll(deadline_at=1001)
    def test_wrong_owner_decode_fails_before_authority(self):
        self.client.emit(self.envelope());results=[]
        def other():
            try:self.transport.decode_delivery(self.tokens[0])
            except H.HostError:results.append('held')
        t=threading.Thread(target=other);t.start();t.join(1);self.assertFalse(t.is_alive());self.assertEqual(results,['held'])
    def test_reply_requires_verified_current_principal_and_exact_topic(self):
        with self.assertRaises(H.HostError):self.transport.send('ito-context/v2/reply/'+PRINCIPAL,b'{}')
        self.transport.verify_broker(self.receive(self.envelope()))
        self.transport.send('ito-context/v2/reply/'+PRINCIPAL,b'{}');self.assertEqual(len(self.client.sent),1)
        with self.assertRaises(H.HostError):self.transport.send('ito-context/v2/reply/other',b'{}')
    def test_clock_rollback_and_expired_delivery_do_not_send(self):
        self.transport.verify_broker(self.receive(self.envelope()));self.clock.advance(2)
        with self.assertRaises(H.HostError):self.transport.send('ito-context/v2/reply/'+PRINCIPAL,b'{}')
        self.assertEqual(self.client.sent,[])
    def test_disconnect_does_not_reconnect_or_retry(self):
        self.client.kw['on_lifecycle_disconnection'](NS());self.client.kw['on_lifecycle_connection_success'](NS(connack_packet=NS(reason_code=0,session_present=False)))
        with self.assertRaises(H.HostError):self.transport.poll(deadline_at=1001)
        self.assertTrue(self.transport.close());self.assertEqual(self.client.stops,1)
    def test_malformed_connection_callback_holds_without_escaping(self):
        self.client.kw['on_lifecycle_connection_success'](NS())
        with self.assertRaises(H.HostError):self.transport.poll(deadline_at=1001)
        self.assertEqual(self.client.sent,[])
    def test_wall_stall_decode_denied(self):
        self.client.emit(self.envelope());self.clock.mono+=3
        with self.assertRaises(H.HostError):self.transport.decode_delivery(self.tokens.pop(0))
    def test_wall_stall_verify_denied(self):
        handle=self.receive(self.envelope());self.clock.mono+=3
        with self.assertRaises(H.HostError):self.transport.verify_broker(handle)
    def test_wall_stall_send_denied(self):
        self.transport.verify_broker(self.receive(self.envelope()));self.clock.mono+=3
        with self.assertRaises(H.HostError):self.transport.send('ito-context/v2/reply/'+PRINCIPAL,b'{}')

class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.typed=b'{"task_id":"job","schema":1}';self.spec=b'{"schema_version":1}';self.env=dict(spec_sha256=hashlib.sha256(self.typed).hexdigest(),claim_owner='operator',dispatcher_id='dispatcher',task_id='job',attempt_id='a'*32)
        self.receipt=dict(principal='reviewer',kind='reviewer',expires_at=1030,request_spec_sha256=hashlib.sha256(self.spec).hexdigest(),envelope=self.env)
        self.owner=NS(canonical=NS(reviewers={'reviewer'},_attempt=lambda e:dict(state='approved_effects_unknown',admitted=0)))
    def adapter(self):return H.ExistingApproval(resolve=lambda **kw:copy.deepcopy(self.receipt),clock=self.clock.time,monotonic=self.clock.monotonic)
    def test_consumes_existing_independent_approval_once(self):
        a=self.adapter();self.assertEqual(a(self.owner,self.typed,self.spec,deadline_at=1020),self.env)
        with self.assertRaises(H.HostError):a(self.owner,self.typed,self.spec,deadline_at=1020)
    def test_wrong_reviewer_spec_expiry_or_state_holds(self):
        for patch in [dict(principal='dispatcher'),dict(kind='worker'),dict(expires_at=999),dict(request_spec_sha256='0'*64)]:
            with self.subTest(patch=patch):
                old=self.receipt;self.receipt={**old,**patch}
                with self.assertRaises(H.HostError):self.adapter()(self.owner,self.typed,self.spec,deadline_at=1020)
                self.receipt=old
    def test_missing_approval_cannot_self_approve(self):
        a=H.ExistingApproval(resolve=lambda **kw:None,clock=self.clock.time,monotonic=self.clock.monotonic)
        with self.assertRaises(H.HostError):a(self.owner,self.typed,self.spec,deadline_at=1020)
    def test_existing_admitted_attempt_cannot_be_reapproved(self):
        self.owner.canonical._attempt=lambda e:dict(state='admitted_effects_unknown',admitted=1)
        with self.assertRaises(H.HostError):self.adapter()(self.owner,self.typed,self.spec,deadline_at=1020)



import tempfile
import datetime
# Vendored, hash-verified fixture inputs (see tests/fixtures/native_caller/PINS.json).
REPO=ROOT/'tests'/'fixtures'/'native_caller'
_pins=json.loads((REPO/'PINS.json').read_text())['files']
for _rel,_want in _pins.items():
    _got=hashlib.sha256((REPO/_rel).read_bytes()).hexdigest()
    if _got!=_want:raise RuntimeError('native_caller fixture pin mismatch: '+_rel)
sys.path.insert(0,str(REPO))
spec=importlib.util.spec_from_file_location('launch_fixture',REPO/'tests/test_context_launch.py')
LF=importlib.util.module_from_spec(spec);spec.loader.exec_module(LF)
from infra.aws.exchange.artifacts import ArtifactStore

class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();fixture=LF.LaunchPolicy();fixture.setUp();self.fixture=fixture
        self.policy=fixture.policy;self.task=fixture.observation();self.definition=LF.definition();p=LF.POLICY
        self.task['attachments']=[dict(type='ElasticNetworkInterface',details=[dict(name='networkInterfaceId',value='eni-'+'a'*17)])]
        self.task['createdAt']=datetime.datetime.now(datetime.timezone.utc)
        self.role=dict(Arn=p['task_role_arn'],RoleId=p['task_role_id'],CreateDate=datetime.datetime.now(datetime.timezone.utc))
        self.eni=dict(NetworkInterfaceId='eni-'+'a'*17,OwnerId=p['account'],SubnetId=p['subnet_id'],Groups=[dict(GroupId=p['security_group_id'])])
        self.calls=[]
        def call(service,operation,**kw):
            self.calls.append((service,operation,kw))
            return copy.deepcopy({'describe_tasks':{'tasks':[self.task]},'describe_task_definition':{'taskDefinition':self.definition},'get_role':{'Role':self.role},'describe_network_interfaces':{'NetworkInterfaces':[self.eni]}}[operation])
        self.observer=H.ECSObserver(call=call,policy=self.policy,coordinates=LF.f.COORDS,deadline_at=1030,clock=self.clock.time,monotonic=self.clock.monotonic)
    def launch(self):return self.observer.observe_launch({'tasks':[{'taskArn':self.task['taskArn']}],'failures':[]},LF.f.COORDS,deadline_at=1020)
    def test_actual_v2_policy_accepts_fresh_api_projection(self):
        self.task['tags'].reverse();v=self.launch()
        self.assertEqual(set(v),{'task','task_definition','role','network'});self.assertEqual(len(self.calls),4)
        self.assertNotIn('createdAt',v['task']);self.assertEqual(self.observer.executor['task_arn'],self.task['taskArn'])
    def test_wrong_role_image_network_or_launch_tag_denied(self):
        for target,key,value in [(self.role,'RoleId','AROA'+'B'*17),(self.eni,'OwnerId','999999999999'),(self.eni,'Association',{'PublicIp':'192.0.2.3'}),(self.task,'startedBy','wrong')]:
            with self.subTest(key=key):
                old=target.get(key);target[key]=value
                with self.assertRaises(H.HostError):self.launch()
                if old is None:target.pop(key)
                else:target[key]=old
    def test_eni_id_must_be_observed_from_task(self):
        self.task['attachments']=[]
        with self.assertRaises(H.HostError):self.launch()
        self.assertFalse(any(op=='describe_network_interfaces' for _,op,_ in self.calls))
    def test_stopped_exit_failure_not_success(self):
        self.launch();self.task['lastStatus']='STOPPED';self.task['stopCode']='EssentialContainerExited';self.task['containers'][0]['exitCode']=1
        with self.assertRaises(H.HostError):self.observer.terminal(self.observer.executor,deadline_at=1020)
    def test_stopped_exact_executor_corrobates_exit_only(self):
        self.launch();self.task['lastStatus']='STOPPED';self.task['stopCode']='EssentialContainerExited';self.task['containers'][0]['exitCode']=0
        value=self.observer.terminal(self.observer.executor,deadline_at=1020)
        self.assertEqual(set(value),{'executor','exit_code'});self.assertNotIn('cleanup_complete',value)
    def test_running_not_terminal(self):
        self.launch();self.assertIsNone(self.observer.terminal(self.observer.executor,deadline_at=1020))
    def test_sdk_unknown_cardinality_or_failure_holds(self):
        for response in [{'tasks':[]},{'tasks':[self.task,self.task]},{'tasks':[self.task],'failures':[{}]}]:
            with self.subTest(response=list(response)):
                with self.assertRaises(H.HostError):self.observer.observe_launch(response,LF.f.COORDS,deadline_at=1020)
    def test_elapsed_readback_cannot_activate(self):
        original=self.observer.call
        def delayed(*a,**kw):r=original(*a,**kw);self.clock.advance(30);return r
        self.observer.call=delayed
        with self.assertRaises(H.HostError):self.launch()
        self.assertIsNone(self.observer.executor)

class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name).resolve();self.clock=Clock()
        self.artifacts=ArtifactStore(self.root,clock=self.clock.time);self.owner=NS(artifacts=self.artifacts)
        self.binding={'synthetic':'owned'};self.other={'synthetic':'other'}
        self.artifacts.publish(self.binding,'input',b'private fixture',created_at=1000,expires_at=1600)
        self.artifacts.publish(self.other,'input',b'other owner',created_at=1000,expires_at=1600)
        self.cleanup=H.ArtifactCleanup(clock=self.clock.time,monotonic=self.clock.monotonic)
    def tearDown(self):self.artifacts.close();self.tmp.cleanup()
    def test_actual_store_removes_only_this_binding_and_preserves_sibling(self):
        othername,_=self.artifacts._key(self.other,'input');ownname,_=self.artifacts._key(self.binding,'input')
        before=(self.root/othername).read_bytes()
        self.assertTrue(self.cleanup(self.owner,None,self.binding,deadline_at=1020))
        self.assertFalse((self.root/ownname).exists());self.assertEqual((self.root/othername).read_bytes(),before);self.assertTrue((self.root/'.lock').exists())
    def test_symlink_or_foreign_file_holds_without_recursive_deletion(self):
        (self.root/'foreign').write_bytes(b'keep')
        self.assertFalse(self.cleanup(self.owner,None,self.binding,deadline_at=1020));self.assertEqual((self.root/'foreign').read_bytes(),b'keep')
    def test_live_transport_prevents_artifact_deletion(self):
        event=threading.Event();self.assertFalse(self.cleanup(self.owner,NS(_closed=True,_stopped=event),self.binding,deadline_at=1020))
        name,_=self.artifacts._key(self.binding,'input');self.assertTrue((self.root/name).exists())
    def test_corrupt_target_holds_before_delete(self):
        name,_=self.artifacts._key(self.binding,'input');(self.root/name).write_bytes(b'bad')
        self.assertFalse(self.cleanup(self.owner,None,self.binding,deadline_at=1020));self.assertTrue((self.root/name).exists())
        self.assertEqual(self.artifacts._fd,-1);self.assertEqual(self.artifacts._lockfd,-1)

class TerminalTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.env={'attempt_id':'a'*32};self.executor={'task_arn':'synthetic'};self.binding={'canonical_sha256':H.digest(H.encoded(self.env)),'spec_sha256':'b'*64,'executor':self.executor}
        self.summary={'exit_code':0,'timed_out':False,'cleanup_complete':True,'capture_complete':True,'stdout_bytes':2,'stderr_bytes':0,'stdout_sha256':H.digest(b'{}'),'stderr_sha256':H.digest(b''),'duration_s':1.0}
        self.imports=[];self.reads=[]
        def inspect(env,read):
            if read('terminal') is None:raise ValueError('missing')
            return dict(state='execution_succeeded_pending_review',result=self.summary)
        def import_receipts(env,read):self.imports.append(env);read('terminal');return dict(state='execution_succeeded_pending_review',retryable=False)
        self.owner=NS(canonical=NS(inspect_receipts=inspect,import_receipts=import_receipts,_attempt=lambda e:dict(state='execution_succeeded_pending_review',admitted=1)))
        self.observer=NS(executor=self.executor,terminal=lambda *a,**k:dict(executor=self.executor,exit_code=0))
        self.exchange=NS(read_process_summary=lambda b:dict(summary=copy.deepcopy(self.summary)))
        self.port=H.TerminalReceipts(approval=NS(envelope=self.env),observer=self.observer,read_receipt=lambda key,**kw:self.reads.append(key) or b'{}',clock=self.clock.time,monotonic=self.clock.monotonic)
    def test_external_receipt_import_uses_exact_summary_without_accepting(self):
        value=self.port.corroborate_terminal(self.owner,self.exchange,self.binding,deadline_at=1020)
        self.assertIs(value['acceptance_verified'],False);self.assertEqual(value['result'],self.summary);self.assertEqual(len(self.imports),1);self.assertEqual(self.reads,['terminal'])
        self.assertEqual(self.port.reconcile(self.owner,self.env,self.executor,deadline_at=1020),{'resolved':True})
    def test_worker_summary_and_ecs_zero_without_external_receipt_hold(self):
        self.port.read=lambda *a,**k:None
        with self.assertRaises(H.HostError):self.port.corroborate_terminal(self.owner,self.exchange,self.binding,deadline_at=1020)
        self.assertEqual(self.imports,[])
    def test_contradicted_summary_not_imported(self):
        self.exchange.read_process_summary=lambda b:dict(summary={**self.summary,'stdout_bytes':9})
        with self.assertRaises(H.HostError):self.port.corroborate_terminal(self.owner,self.exchange,self.binding,deadline_at=1020)
        self.assertEqual(self.imports,[])
    def test_running_does_not_read_or_import_terminal(self):
        self.observer.terminal=lambda *a,**k:None
        self.assertIsNone(self.port.corroborate_terminal(self.owner,self.exchange,self.binding,deadline_at=1020));self.assertEqual(self.reads,[])
    def test_cached_terminal_cannot_cross_binding(self):
        self.port.corroborate_terminal(self.owner,self.exchange,self.binding,deadline_at=1020)
        with self.assertRaises(H.HostError):self.port.corroborate_terminal(self.owner,self.exchange,{**self.binding,'canonical_sha256':'f'*64},deadline_at=1020)
    def test_cached_terminal_cannot_move_to_another_thread(self):
        self.port.corroborate_terminal(self.owner,self.exchange,self.binding,deadline_at=1020);results=[]
        def other():
            try:self.port.corroborate_terminal(self.owner,self.exchange,self.binding,deadline_at=1020)
            except H.HostError:results.append('held')
        t=threading.Thread(target=other);t.start();t.join(1);self.assertFalse(t.is_alive());self.assertEqual(results,['held'])
    def test_other_attempt_reconcile_never_releases(self):
        self.assertEqual(self.port.reconcile(self.owner,{'other':1},self.executor,deadline_at=1020),{'resolved':False})

class FactoryTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.agent=object();self.calls=[];self.valid=True
        self.request=dict(host_caller=object(),controller_caller=object(),run_kwargs={'task_id':'job'},expires_at=1020)
        self.outcome=NS(state='completed',context_result={'private':'must project'},closure_receipt={'cleanup_complete':True,'durable_capacity_released':False})
        owner=self
        class Dispatcher:
            def __init__(self,**kw):owner.calls.append(('construct',kw))
            def run(self,**kw):owner.calls.append(('run',kw));return owner.outcome
        self.factory=H.prepared_factory(prepared_class=NS,dispatcher_class=Dispatcher,resolve_request=lambda a:self.request,
            make_bundle=lambda r:NS(ports=lambda:object()),validate_request=lambda a,r:self.valid,
            project_context=lambda a,r,c:'approved context',clock=self.clock.time,monotonic=self.clock.monotonic)
    def test_factory_inert_then_one_exact_invoke_and_safe_projection(self):
        p=self.factory(agent=self.agent);self.assertEqual(self.calls,[]);result=p.invoke();self.assertEqual(len(self.calls),2)
        self.assertEqual(p.project_result(result),{'status':'completed','context':'approved context'})
        with self.assertRaises(H.HostError):p.invoke()
    def test_revoked_request_no_construction(self):
        p=self.factory(agent=self.agent);self.valid=False
        with self.assertRaises(H.HostError):p.invoke()
        self.assertEqual(self.calls,[])
    def test_forged_outcome_or_incomplete_cleanup_never_projected(self):
        p=self.factory(agent=self.agent);p.invoke()
        with self.assertRaises(H.HostError):p.project_result(copy.copy(self.outcome))
        self.outcome.closure_receipt['cleanup_complete']=False
        with self.assertRaises(H.HostError):p.project_result(self.outcome)



class BotoTests(unittest.TestCase):
    def test_explicit_session_bounded_config_and_closed_client(self):
        seen=[];closed=[];clock=Clock()
        client=NS(describe_tasks=lambda **kw:seen.append(kw) or {'tasks':[]},close=lambda:closed.append(True))
        def create(service,**kw):seen.append((service,kw));return client
        call=H.BotoCalls(session=NS(client=create),clock=clock.time,monotonic=clock.monotonic,config_factory=lambda **kw:kw)
        self.assertEqual(call('ecs','describe_tasks',deadline_at=1003,tasks=['id']),{'tasks':[]})
        config=seen[0][1]['config'];self.assertEqual(config['retries']['total_max_attempts'],1)
        self.assertLessEqual(config['connect_timeout']+config['read_timeout'],3);self.assertEqual(closed,[True])
    def test_provider_exception_is_fixed_and_client_closed(self):
        closed=[]
        def fail(**kw):raise RuntimeError('synthetic-private-provider-text')
        client=NS(run_task=fail,close=lambda:closed.append(True));clock=Clock()
        call=H.BotoCalls(session=NS(client=lambda *a,**kw:client),clock=clock.time,monotonic=clock.monotonic,config_factory=lambda **kw:kw)
        with self.assertRaisesRegex(H.HostError,'^provider_outcome_unknown$'):call('ecs','run_task',deadline_at=1003)
        self.assertEqual(closed,[True]);self.assertEqual(call.calls,1)

if __name__=='__main__':unittest.main()
