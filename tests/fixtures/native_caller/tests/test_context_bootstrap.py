import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from infra.aws.runner import context_bootstrap as c

COORDS={"attempt_id":"1"*32,"request_id":"2"*32,"request_spec_sha256":"3"*64,
        "spec_sha256":"4"*64,"input_sha256":"5"*64,"source_sha256":"6"*64,
        "canonical_sha256":"7"*64,"launch_token":"8"*32}
CONFIG=dict(account="123456789012",region="us-east-1",
    cluster_arn="arn:aws:ecs:us-east-1:123456789012:cluster/synthetic",
    family="ItoDeskContextExchangeV2",container_name="context-exchange-v1")
TASK_ARN="arn:aws:ecs:us-east-1:123456789012:task/synthetic/"+"a"*32
URI="http://169.254.170.2/v4/synthetic-container-id"

def native():
    return dict(Cluster=CONFIG["cluster_arn"],TaskARN=TASK_ARN,Family=CONFIG["family"],Revision="7",
        Containers=[dict(Type="NORMAL",Name=CONFIG["container_name"],Image="synthetic-image",ImageID="sha256:"+"b"*64)],
        AvailabilityZone="us-east-1a")

def encoded(value):return json.dumps(value,separators=(",",":"),sort_keys=True).encode()

class Coordinates(unittest.TestCase):
    def test_exact_canonical_public_override(self):
        self.assertEqual(c.parse_coordinates(encoded(COORDS)),COORDS)
    def test_extra_private_field_rejected(self):
        with self.assertRaises(ValueError):c.parse_coordinates(encoded(dict(COORDS,input="private")))
    def test_duplicate_field_rejected(self):
        raw=encoded(COORDS);raw=raw[:-1]+b',"attempt_id":"'+b'1'*32+b'"}'
        with self.assertRaises(ValueError):c.parse_coordinates(raw)
    def test_missing_or_noncanonical_hash_rejected(self):
        for value in [dict(COORDS,request_spec_sha256="A"*64),{k:v for k,v in COORDS.items() if k!="launch_token"}]:
            with self.subTest(value=value),self.assertRaises(ValueError):c.parse_coordinates(encoded(value))
    def test_oversize_override_rejected(self):
        with self.assertRaises(ValueError):c.parse_coordinates(b" "*4097)

class NativeMetadata(unittest.TestCase):
    def test_native_identity_with_documented_additive_fields(self):
        result=c.parse_native_metadata(encoded(native()),**CONFIG)
        self.assertEqual(result["task_arn"],TASK_ARN)
        self.assertEqual(result["task_id"],"a"*32)
        self.assertEqual(result["task_definition_arn"],"arn:aws:ecs:us-east-1:123456789012:task-definition/ItoDeskContextExchangeV2:7")
        self.assertEqual(result["image_digest"],"sha256:"+"b"*64)
    def test_other_task_account_region_or_cluster_rejected(self):
        for changes in [dict(TaskARN=TASK_ARN.replace("123456789012","999999999999")),dict(TaskARN=TASK_ARN.replace("us-east-1","us-west-2")),dict(Cluster="other")]:
            with self.subTest(changes=changes),self.assertRaises(ValueError):c.parse_native_metadata(encoded(dict(native(),**changes)),**CONFIG)
    def test_conflicting_container_or_revision_rejected(self):
        for changes in [dict(Revision=True),dict(Revision="0"),dict(Containers=[]),dict(Containers=native()["Containers"]*2),dict(Family="other")]:
            with self.subTest(changes=changes),self.assertRaises(ValueError):c.parse_native_metadata(encoded(dict(native(),**changes)),**CONFIG)
    def test_uri_redirect_proxy_and_alternate_host_denied_before_connector(self):
        for uri in ["http://169.254.170.2.evil/v4/id",URI+"/../other",URI+"?q=x",URI.replace("http:","https:"),"http://localhost/v4/id"]:
            with self.subTest(uri=uri),self.assertRaises(ValueError):
                c.read_native_metadata({"ECS_CONTAINER_METADATA_URI_V4":uri},config=CONFIG,clock=lambda:10,deadline=12,connector=lambda *a,**k:self.fail("connector called"))
    def reader(self,response=None,step=0):
        now=[10.0];observed=[]
        body=encoded(native())
        raw=response if response is not None else b"HTTP/1.1 200 OK\r\nContent-Length: "+str(len(body)).encode()+b"\r\n\r\n"+body
        class Socket:
            def __init__(self):self.raw=raw;self.closed=False;self.sent=b""
            def settimeout(self,value):observed.append(("timeout",value))
            def sendall(self,value):self.sent=value
            def recv(self,size):
                now[0]+=step
                block,self.raw=self.raw[:size],self.raw[size:]
                return block
            def close(self):self.closed=True
        sock=Socket()
        def connector(address,timeout):observed.append((address,timeout));return sock
        return now,observed,sock,connector
    def test_reader_fixed_link_local_request_and_cleanup(self):
        now,seen,sock,connector=self.reader()
        result=c.read_native_metadata({"ECS_CONTAINER_METADATA_URI_V4":URI},config=CONFIG,clock=lambda:now[0],deadline=12,connector=connector)
        self.assertEqual(result["task_arn"],TASK_ARN)
        self.assertEqual(seen[0],(("169.254.170.2",80),2))
        self.assertTrue(sock.sent.startswith(b"GET /v4/synthetic-container-id/task HTTP/1.1\r\n"))
        self.assertTrue(sock.closed)
    def test_reader_does_not_follow_http_redirect(self):
        now,seen,sock,connector=self.reader(b"HTTP/1.1 302 Found\r\nLocation: http://elsewhere/\r\nContent-Length: 0\r\n\r\n")
        with self.assertRaises(ValueError):c.read_native_metadata({"ECS_CONTAINER_METADATA_URI_V4":URI},config=CONFIG,clock=lambda:now[0],deadline=12,connector=connector)
        self.assertTrue(sock.closed)
        self.assertEqual(sum(isinstance(v[0],tuple) for v in seen),1)
    def test_reader_enforces_overall_deadline(self):
        now,seen,sock,connector=self.reader(step=3)
        with self.assertRaises(ValueError):c.read_native_metadata({"ECS_CONTAINER_METADATA_URI_V4":URI},config=CONFIG,clock=lambda:now[0],deadline=12,connector=connector)
        self.assertTrue(sock.closed)
    def test_reader_rejects_clock_rollback(self):
        now,seen,sock,connector=self.reader(step=-1)
        with self.assertRaises(ValueError):c.read_native_metadata({"ECS_CONTAINER_METADATA_URI_V4":URI},config=CONFIG,clock=lambda:now[0],deadline=12,connector=connector)
        self.assertTrue(sock.closed)
    def test_reader_rejects_body_over_cap(self):
        now,seen,sock,connector=self.reader(b"HTTP/1.1 200 OK\r\nContent-Length: 65537\r\n\r\n"+b"x"*65537)
        with self.assertRaises(ValueError):c.read_native_metadata({"ECS_CONTAINER_METADATA_URI_V4":URI},config=CONFIG,clock=lambda:now[0],deadline=12,connector=connector)
        self.assertTrue(sock.closed)

class NativeDocumentedForms(unittest.TestCase):
    def test_short_cluster_name_with_matching_full_task_arn(self):
        value=native();value["Cluster"]="synthetic"
        self.assertEqual(c.parse_native_metadata(encoded(value),**CONFIG)["task_arn"],TASK_ARN)
    def test_internal_agent_container_does_not_replace_normal_container(self):
        value=native();value["Containers"].append(dict(Type="CNI_PAUSE",Name="internal"))
        self.assertEqual(c.parse_native_metadata(encoded(value),**CONFIG)["container_name"],CONFIG["container_name"])
    def test_missing_container_type_rejected(self):
        value=native();del value["Containers"][0]["Type"]
        with self.assertRaises(ValueError):c.parse_native_metadata(encoded(value),**CONFIG)
    def test_socket_close_error_has_fixed_boundary(self):
        helper=NativeMetadata(methodName="runTest")
        now,seen,sock,connector=helper.reader()
        def bad_close():
            sock.closed=True
            raise OSError("synthetic private close failure")
        sock.close=bad_close
        with self.assertRaisesRegex(ValueError,"^native metadata unavailable$"):
            c.read_native_metadata({"ECS_CONTAINER_METADATA_URI_V4":URI},config=CONFIG,clock=lambda:now[0],deadline=12,connector=connector)
        self.assertTrue(sock.closed)

class BootstrapPolling(unittest.TestCase):
    def setUp(self):
        import hashlib
        self.now=[10.0];self.wall=[1700000000.0]
        self.native=c.parse_native_metadata(encoded(native()),**CONFIG)
        self.typed=dict(schema=1,operation="nonpricing.context",task_id="synthetic-task",request_spec_sha256=COORDS["request_spec_sha256"],timeout=30)
        self.coords=dict(COORDS,spec_sha256=hashlib.sha256(encoded(self.typed)).hexdigest())
        self.binding={k:self.coords[k] for k in ("request_id","request_spec_sha256","canonical_sha256","spec_sha256","input_sha256","source_sha256")}
        self.binding.update(manifest_sha256="9"*64,executor=dict(task_arn=TASK_ARN,task_definition_arn=self.native["task_definition_arn"],launch_token=COORDS["launch_token"]))
        owner=self
        class Link:
            def __init__(self):self.sent=[];self.held=False;self.completed=False;self.receiver=None;self.callback=None
            def set_disconnect_callback(self,callback):self.callback=callback
            def send(self,body):self.sent.append(json.loads(body))
            def receive_bootstrap(self,deadline):return self.receiver(deadline)
            def complete_bootstrap(self):self.completed=True
            def abort_bootstrap(self):self.held=True
        self.link=Link()
        self.gate=c.BootstrapPoll(self.coords,self.native,clock=lambda:self.now[0],wall_clock=lambda:self.wall[0],started_at=10,sleep=self.sleep)
    def sleep(self,seconds):self.now[0]+=seconds;self.wall[0]+=seconds
    def reply(self,request=None,**changes):
        request=request or self.link.sent[-1]
        value=dict(schema=2,attempt_id=request["attempt_id"],request_id=request["request_id"],
            connection_generation=None,invocation_id=None,rpc_id=request["rpc_id"],verb="BOOTSTRAP_INFO",
            manifest_sha256=self.binding["manifest_sha256"],data=dict(boot_nonce=request["data"]["boot_nonce"],binding=self.binding,
            typed_payload=self.typed,deadline_at=1700000120.0,expires_at=self.wall[0]+1))
        value.update(changes);return encoded(value)
    def test_metadata_success_creates_no_operational_request(self):
        self.link.receiver=lambda deadline:self.reply()
        result=self.gate.bind(self.link)
        self.assertEqual(result["binding"],self.binding)
        self.assertEqual(result["typed_payload"],self.typed)
        self.assertEqual([v["verb"] for v in self.link.sent],["BOOTSTRAP"])
        self.assertTrue(self.link.completed)
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertEqual(len(self.link.sent),1)
    def test_preactivation_timeout_then_fresh_rpc_succeeds(self):
        def receive(deadline):
            if len(self.link.sent)==1:self.sleep(deadline-self.now[0]);return None
            return self.reply()
        self.link.receiver=receive
        self.gate.bind(self.link)
        self.assertEqual(len(self.link.sent),2)
        self.assertNotEqual(self.link.sent[0]["rpc_id"],self.link.sent[1]["rpc_id"])
        self.assertNotEqual(self.link.sent[0]["data"]["boot_nonce"],self.link.sent[1]["data"]["boot_nonce"])
        self.assertGreaterEqual(self.now[0],12.25)
    def test_late_issued_rpc_discarded_then_current_reply_accepted(self):
        delivered=[False]
        def receive(deadline):
            if len(self.link.sent)==1:self.sleep(deadline-self.now[0]);return None
            if not delivered[0]:delivered[0]=True;return self.reply(self.link.sent[0])
            return self.reply()
        self.link.receiver=receive
        self.gate.bind(self.link)
        self.assertEqual(len(self.link.sent),2)
    def test_unissued_rpc_holds(self):
        self.link.receiver=lambda deadline:self.reply(rpc_id="f"*32)
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertTrue(self.link.held);self.assertEqual(len(self.link.sent),1)
    def test_invalid_nonce_holds(self):
        def receive(deadline):
            response=json.loads(self.reply());response["data"]["boot_nonce"]="f"*32;return encoded(response)
        self.link.receiver=receive
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertTrue(self.link.held)
    def test_native_task_mismatch_holds(self):
        self.binding["executor"]["task_arn"]=TASK_ARN[:-1]+"b"
        self.link.receiver=lambda deadline:self.reply()
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertTrue(self.link.held)
    def test_typed_payload_hash_mismatch_holds(self):
        self.typed["timeout"]=1
        self.link.receiver=lambda deadline:self.reply()
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertTrue(self.link.held)
    def test_operational_frame_during_bootstrap_holds(self):
        self.link.receiver=lambda deadline:self.reply(verb="INPUT")
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertTrue(self.link.held)
    def test_expired_metadata_holds(self):
        def receive(deadline):
            response=json.loads(self.reply());response["data"]["expires_at"]=self.wall[0];return encoded(response)
        self.link.receiver=receive
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertTrue(self.link.held)
    def test_total_initialization_window_includes_prior_work(self):
        self.now[0]=40
        self.link.receiver=lambda deadline:self.fail("read beyond total window")
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertEqual(self.link.sent,[])
    def test_poll_count_and_total_time_remain_bounded(self):
        def receive(deadline):self.sleep(deadline-self.now[0]);return None
        self.link.receiver=receive
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertLessEqual(len(self.link.sent),16)
        self.assertLessEqual(self.now[0],40)
        self.assertTrue(self.link.held)
    def test_disconnect_never_restarts_bootstrap(self):
        def receive(deadline):self.link.callback();return None
        self.link.receiver=receive
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertEqual(len(self.link.sent),1)
        self.assertTrue(self.link.held)
    def test_clock_rollback_holds(self):
        def receive(deadline):self.now[0]=9;return self.reply()
        self.link.receiver=receive
        with self.assertRaises(ValueError):self.gate.bind(self.link)
        self.assertTrue(self.link.held)

class BootstrapTransport(unittest.TestCase):
    def setUp(self):
        import hashlib
        import importlib.util
        from unittest.mock import patch
        path=Path(__file__).with_name("context_worker_fixtures.py")
        spec=importlib.util.spec_from_file_location("frozen_mqtt_fixture",path)
        fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
        self.f=fixture.MqttCases(methodName="runTest")
        with patch.object(fixture.w,"MqttLink",c.BootstrapMqttLink):self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.link=self.f.link;self.link.connect()
    def test_bootstrap_timeout_leaves_uninterrupted_link_usable(self):
        self.assertIsNone(self.link.receive_bootstrap(self.f.now[0]))
        self.assertFalse(self.link._held)
        self.link.send(self.f.body)
        self.assertEqual(len(self.f.sent),1)
    def test_operational_timeout_still_holds(self):
        with self.assertRaises(ValueError):self.link.receive(self.f.now[0])
        self.assertTrue(self.link._held)
    def test_bootstrap_receive_returns_only_adapter_checked_bytes(self):
        self.f.deliver()
        self.assertEqual(self.link.receive_bootstrap(2),b"authenticated-response")
    def test_binding_rejects_queued_duplicate_instead_of_hiding_it(self):
        self.f.deliver()
        with self.assertRaises(ValueError):self.link.complete_bootstrap()
        self.assertTrue(self.link._held)
    def test_bootstrap_disabled_after_binding(self):
        self.link.complete_bootstrap()
        with self.assertRaises(ValueError):self.link.receive_bootstrap(2)
        self.assertFalse(self.link._held)
    def test_disconnect_holds_bootstrap_receive(self):
        self.f.callbacks["disconnect"](None)
        with self.assertRaises(ValueError):self.link.receive_bootstrap(2)
        self.assertTrue(self.link._held)

if __name__=="__main__":unittest.main(verbosity=2)
