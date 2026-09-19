"""Portable fixture helpers extracted from reviewed worker cases; no test methods."""
import sys
import types
import unittest
from pathlib import Path
from infra.aws.runner import context_exchange_v1 as w
ROLE="AROA"+"A"*17
TASK="1"*32

class ProcessCases(unittest.TestCase):
    def setUp(self):
        import hashlib
        import json
        import tempfile
        import time
        self.cli_source=Path(__file__).resolve().parents[1]
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)
        self.now=time.time()
        self.raw=lambda value:json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode()
        self.sha=lambda value:hashlib.sha256(value).hexdigest()
        self.input=self.raw({"generated_at":"2023-11-14T22:13:20Z","source":{"observation":"canonical_book_only"},"inventory":[{"inventory_id":"synthetic-row","version":1,"has_or_needs":"NEEDS","company_id":"synthetic-company","gpu":{"exact_sku":"B300"},"quantity":{"amount":8,"unit":"gpus"},"delivery":{"region":"US East","required_by":"2026-10-01"},"tracking":{"status":"unconfirmed"}}],"matches":{"matches":[]}})
        self.spec=dict(schema_version=1,request_id="1"*32,operation="nonpricing.context",input_sha256=self.sha(self.input),input_bytes=len(self.input),cli_source_manifest_sha256=self.sha(self.raw(w.CLI_PINS)),audience_grant_sha256="2"*64,projection=dict(company_id="synthetic-company",evaluation_time=1700000000,limit=20),deadline_at=self.now+120,result_max_bytes=8192)
        self.typed=dict(schema=1,operation="nonpricing.context",task_id="synthetic-task",request_spec_sha256=self.sha(self.raw(self.spec)),timeout=30)
        self.binding=dict(request_id="1"*32,request_spec_sha256=self.typed["request_spec_sha256"],manifest_sha256="3"*64,canonical_sha256="4"*64,spec_sha256=self.sha(self.raw(self.typed)),input_sha256=self.spec["input_sha256"],source_sha256=self.spec["cli_source_manifest_sha256"],executor=dict(task_arn="synthetic-task",task_definition_arn="synthetic-definition",launch_token="5"*32))

class MqttCases(unittest.TestCase):
    def setUp(self):
        from dataclasses import dataclass
        from enum import IntEnum
        self.now = [1.0]
        self.settings = types.SimpleNamespace(client_id=ROLE+":"+TASK,
                            rejoined_session=False, session_expiry_interval_sec=0)
        self.suback = [0]
        self.subscribe_delay = 0
        self.publish_error = False
        self.connection_failure = False
        self.sent = []
        self.subscriptions = []
        self.stops = []
        self.timeouts = []
        owner = self
        class QoS(IntEnum): AT_MOST_ONCE = 0
        class Retain(IntEnum): DONT_SEND = 2
        @dataclass
        class Subscription:
            topic_filter: str
            qos: object
            retain_as_published: bool
            retain_handling_type: object
        @dataclass
        class SubscribePacket:
            subscriptions: list
        @dataclass
        class PublishPacket:
            payload: bytes
            topic: str
            qos: object
            retain: bool
            message_expiry_interval_sec: int
        class Future:
            def __init__(self, kind): self.kind = kind
            def result(self, *, timeout):
                owner.timeouts.append(timeout)
                if self.kind == "subscribe":
                    owner.now[0] += owner.subscribe_delay
                    return types.SimpleNamespace(reason_codes=owner.suback)
                if owner.publish_error:
                    raise RuntimeError("synthetic private transport detail")
                return None
        class Client:
            def start(self):
                if owner.connection_failure:
                    owner.callbacks["failure"](None)
                else:
                    owner.callbacks["connected"](types.SimpleNamespace(negotiated_settings=owner.settings))
            def subscribe(self, packet):
                owner.subscriptions.append(packet)
                return Future("subscribe")
            def publish(self, packet):
                owner.sent.append(packet)
                return Future("publish")
            def stop(self):
                owner.stops.append(True)
                owner.callbacks["stopped"](None)
        def builder(*, endpoint, region, credentials_provider, client_id, port,
                    session_behavior, session_expiry_interval_sec, offline_queue_behavior,
                    maximum_packet_size, will, http_proxy_options, enable_metrics_collection,
                    on_publish_received, on_lifecycle_connection_success,
                    on_lifecycle_disconnection, on_lifecycle_connection_failure,
                    on_lifecycle_stopped):
            self.callbacks = dict(publish=on_publish_received, connected=on_lifecycle_connection_success,
                disconnect=on_lifecycle_disconnection, failure=on_lifecycle_connection_failure,
                stopped=on_lifecycle_stopped)
            return Client()
        mqtt = types.SimpleNamespace(QoS=QoS, RetainHandlingType=Retain,
            Subscription=Subscription, SubscribePacket=SubscribePacket, PublishPacket=PublishPacket,
            SubackReasonCode=types.SimpleNamespace(GRANTED_QOS_0=0),
            ClientSessionBehaviorType=types.SimpleNamespace(CLEAN=1),
            ClientOperationQueueBehaviorType=types.SimpleNamespace(FAIL_ALL_ON_DISCONNECT=3))
        self.link = w.MqttLink(endpoint="synthetic-ats.iot.us-east-1.amazonaws.com", region="us-east-1",
            role_id=ROLE, task_id=TASK, credentials_provider=object(), mqtt5=mqtt,
            builder=types.SimpleNamespace(websockets_with_default_aws_signing=builder), clock=lambda:self.now[0])
        self.addCleanup(self.link.close)
        self.body = w.wire.encode(dict(schema=2,attempt_id="2"*32,request_id="3"*32,
            connection_generation=None,invocation_id=None,rpc_id="4"*32,
            manifest_sha256="5"*64,verb="HELLO",data={"boot_nonce":"6"*32}))
    def deliver(self, **changes):
        values = dict(payload=b"authenticated-response", topic=self.link._reply_topic, qos=0, retain=False)
        values.update(changes)
        self.callbacks["publish"](types.SimpleNamespace(publish_packet=types.SimpleNamespace(**values)))
