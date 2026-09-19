"""Source-only canonical AWS adapter. No network, SDK, execution or default activation.

Caller identities must come from an authenticated control-plane adapter, never
receipt fields. Reviewer/dispatcher allowlists enforce separation, not identity.
"""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import math
import re
import uuid
from .store import Store, StoreError


class ReceiptError(StoreError):
    pass


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',', ':'),ensure_ascii=True,allow_nan=False).encode()


def sha(data): return hashlib.sha256(data).hexdigest()


def text(value, limit=2048):
    if not isinstance(value,str) or not value.strip() or len(value)>limit:
        raise ReceiptError('invalid bounded text field')
    return value


def hex64(value):
    if not isinstance(value,str) or not re.fullmatch('[0-9a-f]{64}',value):
        raise ReceiptError('invalid digest')
    return value


def _pairs(pairs):
    result={}
    for key,value in pairs:
        if key in result: raise ReceiptError('duplicate JSON field')
        result[key]=value
    return result


def parsed(data):
    if not isinstance(data,bytes) or not 0<len(data)<=65536:
        raise ReceiptError('receipt must be bounded UTF-8 JSON bytes')
    try:
        value=json.loads(data.decode('utf-8'),object_pairs_hook=_pairs,
                         parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError,UnicodeError,RecursionError) as exc:
        raise ReceiptError('invalid receipt JSON') from exc
    if not isinstance(value,dict): raise ReceiptError('receipt must be an object')
    return value


def fields(value, expected):
    if not isinstance(value,dict) or set(value)!=set(expected):
        raise ReceiptError('receipt fields do not match contract')


def timestamp(value):
    text(value,64)
    try:
        stamp=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
        if stamp.tzinfo is None: raise ValueError()
        return stamp
    except ValueError as exc: raise ReceiptError('invalid receipt timestamp') from exc


def fingerprint(task):
    d=task.to_dict()
    for k in ('last_check_at','last_check_ok','last_check_detail','updated_at'):d.pop(k)
    return sha(encoded(d))


class CanonicalAWS:
    def __init__(self, store: Store, *, reviewers, dispatchers, max_inflight=0):
        self.store=store
        if type(max_inflight) is not int or not 0<=max_inflight<=64:
            raise ReceiptError('invalid bounded capacity policy')
        self.initial_capacity=max_inflight
        self.reviewers=frozenset(reviewers)
        self.dispatchers=frozenset(dispatchers)
        if not self.reviewers or not self.dispatchers or self.reviewers & self.dispatchers:
            raise ReceiptError('separate reviewer and dispatcher identities required')
        for actor in self.reviewers | self.dispatchers:text(actor,256)

    def initialize_for_candidate(self):
        """Explicit opt-in schema on the supplied Store, intended for disposable tests.

        Live migration/activation is not supplied or authorized by this method.
        Every old writer must be excluded before any eventual adoption.
        """
        with self.store._transaction():
            self.store.conn.execute('CREATE TABLE IF NOT EXISTS aws_queue_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), queue_id TEXT NOT NULL)')
            self.store.conn.execute('INSERT OR IGNORE INTO aws_queue_identity VALUES(1,?)',(uuid.uuid4().hex,))
            self.store.conn.execute('CREATE TABLE IF NOT EXISTS aws_limits (singleton INTEGER PRIMARY KEY CHECK(singleton=1), max_inflight INTEGER NOT NULL)')
            self.store.conn.execute('INSERT OR IGNORE INTO aws_limits VALUES(1,?)',(self.initial_capacity,))
            self.store.conn.execute('CREATE TABLE IF NOT EXISTS aws_enrollments (task_id TEXT PRIMARY KEY, state TEXT NOT NULL)')
            self.store.conn.execute('''CREATE TABLE IF NOT EXISTS aws_attempts (
                attempt_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, generation INTEGER NOT NULL,
                envelope TEXT NOT NULL, task_fingerprint TEXT NOT NULL, state TEXT NOT NULL,
                withdrawn INTEGER NOT NULL DEFAULT 0, admitted INTEGER NOT NULL DEFAULT 0,
                executor TEXT, request_sha256 TEXT, fence_authorized INTEGER NOT NULL DEFAULT 0,
                stop_consumed INTEGER NOT NULL DEFAULT 0, claim_sha256 TEXT, started_sha256 TEXT, terminal_sha256 TEXT,
                terminal_result TEXT, version INTEGER NOT NULL DEFAULT 0, UNIQUE(task_id,generation))''')

    def _attempt(self, envelope):
        if not isinstance(envelope,dict) or not isinstance(envelope.get('attempt_id'),str):
            raise ReceiptError('invalid canonical envelope')
        r=self.store.conn.execute('SELECT * FROM aws_attempts WHERE attempt_id=?',(envelope['attempt_id'],)).fetchone()
        if r is None or encoded(json.loads(r['envelope']))!=encoded(envelope):
            raise ReceiptError('unknown or altered canonical binding')
        return dict(r)

    def _independent(self, actor, envelope, executor=None):
        if actor not in self.reviewers or actor in (envelope['claim_owner'],envelope['dispatcher_id']):
            raise ReceiptError('independent authenticated reviewer required')
        if executor and actor==executor['task_arn']:
            raise ReceiptError('executor cannot approve its own outcome')

    def _dispatcher(self, actor, envelope):
        if actor not in self.dispatchers or actor!=envelope['dispatcher_id']:
            raise ReceiptError('wrong dispatcher identity')

    def _other_unknown(self, envelope):
        return self.store.conn.execute("SELECT 1 FROM aws_attempts WHERE task_id=? AND attempt_id!=? AND state NOT IN ('accepted','retry_authorized') LIMIT 1",
                                      (envelope['task_id'],envelope['attempt_id'])).fetchone() is not None

    def _current(self, r, envelope):
        t=self.store.get(envelope['task_id'])
        if (not t or t.state!='claimed' or t.kind!='deterministic' or t.owner=='affaan'
                or t.owner!=envelope['claim_owner'] or t.claimed_by!=t.owner
                or t.revision!=envelope['claim_revision'] or t.claim_token!=envelope['claim_token']
                or fingerprint(t)!=r['task_fingerprint'] or self.store.unmet_deps(t)
                or self._other_unknown(envelope)):
            raise ReceiptError('canonical claim/dependencies/generation changed')
        return t

    def _event(self, envelope, event, actor, detail=''):
        self.store._event(envelope['task_id'],'aws-'+event,actor,
                          envelope['attempt_id']+': '+detail)

    def approve(self, task_id, actor, *, expected_revision, spec_sha256,
                launch_policy_sha256, task_definition_arn, dispatcher_id):
        hex64(spec_sha256);hex64(launch_policy_sha256);text(task_definition_arn,512)
        if not re.fullmatch(r'arn:aws:ecs:[a-z0-9-]+:[0-9]{12}:task-definition/[A-Za-z0-9_-]+:[1-9][0-9]*',task_definition_arn):
            raise ReceiptError('immutable task-definition ARN required')
        if dispatcher_id not in self.dispatchers:raise ReceiptError('unknown dispatcher')
        self.store._revision(expected_revision)
        with self.store._transaction():
            t=self.store._required(task_id,expected_revision)
            if (not re.fullmatch('[a-z0-9][a-z0-9_-]{2,79}',task_id) or t.kind!='deterministic'
                    or t.owner=='affaan' or t.state!='claimed' or t.claimed_by!=t.owner
                    or not re.fullmatch('[0-9a-f]{32}',t.claim_token) or self.store.unmet_deps(t)):
                raise ReceiptError('task is not canonically eligible')
            if self.store.conn.execute("SELECT 1 FROM aws_attempts WHERE task_id=? AND state NOT IN ('accepted','retry_authorized')",(task_id,)).fetchone():
                raise ReceiptError('unresolved attempt forbids new approval')
            generation=self.store.conn.execute('SELECT COALESCE(MAX(generation),0)+1 FROM aws_attempts WHERE task_id=?',(task_id,)).fetchone()[0]
            queue_id=self.store.conn.execute('SELECT queue_id FROM aws_queue_identity WHERE singleton=1').fetchone()[0]
            e={'schema':2,'queue_id':queue_id,'task_id':t.id,'attempt_id':uuid.uuid4().hex,
               'generation':generation,'claim_revision':t.revision,'claim_token':t.claim_token,
               'claim_owner':t.owner,'approval_id':uuid.uuid4().hex,'launch_token':uuid.uuid4().hex,
               'spec_sha256':spec_sha256,'target_scope':{'area':t.area,'source_ref':t.source_ref,'title':t.title},
               'launch_policy_sha256':launch_policy_sha256,'task_definition_arn':task_definition_arn,
               'dispatcher_id':dispatcher_id}
            self._independent(actor,e)
            self.store.conn.execute('INSERT INTO aws_attempts(attempt_id,task_id,generation,envelope,task_fingerprint,state) VALUES(?,?,?,?,?,?)',
                                    (e['attempt_id'],task_id,generation,encoded(e).decode(),fingerprint(t),'approved_effects_unknown'))
            self.store.conn.execute("INSERT INTO aws_enrollments VALUES(?,'active') ON CONFLICT(task_id) DO UPDATE SET state='active'",(task_id,))
            self._event(e,'approved',actor,spec_sha256)
            return e

    def admit(self, envelope, actor, *, request_sha256=None):
        hex64(request_sha256)
        with self.store._transaction():
            r=self._attempt(envelope);self._dispatcher(actor,envelope);self._current(r,envelope)
            if r['withdrawn'] or r['admitted'] or r['state']!='approved_effects_unknown':
                raise ReceiptError('admission spent or withdrawn')
            limit=self.store.conn.execute('SELECT max_inflight FROM aws_limits WHERE singleton=1').fetchone()[0]
            active=self.store.conn.execute("SELECT COUNT(*) FROM aws_attempts WHERE admitted=1 AND state NOT IN ('accepted','retry_authorized')").fetchone()[0]
            if active>=limit:raise ReceiptError('canonical capacity exhausted or held')
            self.store.conn.execute("UPDATE aws_attempts SET admitted=1,request_sha256=?,state='admitted_effects_unknown',version=version+1 WHERE attempt_id=?",(request_sha256,envelope['attempt_id']))
            self._event(envelope,'admitted',actor)
            return json.loads(r['envelope'])

    def record_launch(self, envelope, actor, executor):
        fields(executor,('task_arn','task_definition_arn','launch_token'))
        text(executor['task_arn'],512)
        if (executor['task_definition_arn']!=envelope['task_definition_arn']
                or executor['launch_token']!=envelope['launch_token']
                or not executor['task_arn'].startswith(':'.join(envelope['task_definition_arn'].split(':')[:5])+':task/')):
            raise ReceiptError('executor identity does not match trusted launch binding')
        with self.store._transaction():
            r=self._attempt(envelope);self._dispatcher(actor,envelope)
            if not r['admitted']:raise ReceiptError('launch was not admitted')
            if r['executor']:
                if encoded(json.loads(r['executor']))!=encoded(executor):raise ReceiptError('executor identity conflict')
                return json.loads(r['executor'])
            if r['state']=='accepted':raise ReceiptError('accepted attempt cannot change identity')
            self.store.conn.execute("UPDATE aws_attempts SET executor=?,state='admitted_effects_unknown',version=version+1 WHERE attempt_id=?",(encoded(executor).decode(),envelope['attempt_id']))
            self._event(envelope,'launch-recorded',actor)
            return json.loads(encoded(executor))

    def withdraw(self, envelope, actor, *, reason):
        text(reason)
        with self.store._transaction():
            r=self._attempt(envelope);self._independent(actor,envelope,json.loads(r['executor']) if r['executor'] else None)
            if r['state']=='accepted':raise ReceiptError('accepted attempt cannot be withdrawn')
            self.store.conn.execute('UPDATE aws_attempts SET withdrawn=1,version=version+1 WHERE attempt_id=?',(envelope['attempt_id'],))
            self._event(envelope,'withdrawn',actor,reason)

    def request_fence(self, envelope, actor, *, reason):
        text(reason)
        with self.store._transaction():
            r=self._attempt(envelope)
            self._independent(actor,envelope,json.loads(r['executor']) if r['executor'] else None)
            if not r['withdrawn'] or not r['executor'] or r['fence_authorized'] or r['state'] in ('accepted','retry_authorized'):
                raise ReceiptError('withdrawn registered attempt and new explicit fence approval required')
            self.store.conn.execute('UPDATE aws_attempts SET fence_authorized=1,version=version+1 WHERE attempt_id=?',(envelope['attempt_id'],))
            self._event(envelope,'fence-approved',actor,sha(encoded(reason)))

    def authorize_stop(self, envelope, actor, executor):
        with self.store._transaction():
            r=self._attempt(envelope);self._dispatcher(actor,envelope)
            if (not r['withdrawn'] or not r['fence_authorized'] or r['stop_consumed'] or not r['executor']
                    or encoded(json.loads(r['executor']))!=encoded(executor) or r['state'] in ('accepted','retry_authorized')):
                raise ReceiptError('exact current one-use fence authority required')
            self.store.conn.execute('UPDATE aws_attempts SET stop_consumed=1,version=version+1 WHERE attempt_id=?',(envelope['attempt_id'],))
            self._event(envelope,'stop-authorized',actor)
            return True

    def _read(self, envelope, read, r):
        task=envelope['task_id'];attempt=envelope['attempt_id'];stem=f'receipts/{task}/{attempt}'
        try:
            blobs={name:read(key) for name,key in [('claim',f'claims/{task}/{attempt}.json'),('started',stem+'/started.json'),('terminal',stem+'/terminal.json')]}
        except Exception:
            raise ReceiptError('receipt read unavailable') from None
        if blobs['claim'] is None:
            if any(blobs[k] is not None for k in ('started','terminal')):raise ReceiptError('receipt without bound claim')
            return {'state':'effects_unknown','retryable':False,'records':{},'result':None}
        claim=parsed(blobs['claim'])
        fields(claim,('schema','task_id','attempt_id','spec_sha256','owner','mode','canonical','executor',*(['created_at'] if 'created_at' in claim else [])))
        wanted={'schema':2,'task_id':task,'attempt_id':attempt,'spec_sha256':envelope['spec_sha256'],
                'owner':envelope['dispatcher_id'],'mode':'queue','canonical':envelope,'executor':None}
        if encoded({k:claim[k] for k in wanted})!=encoded(wanted):raise ReceiptError('claim binding mismatch or legacy claim')
        start_time=timestamp(claim['created_at']) if 'created_at' in claim else None
        records={'claim':sha(blobs['claim'])};result=None
        binding={'schema':2,'task_id':task,'attempt_id':attempt,'spec_sha256':envelope['spec_sha256'],
                 'claim_sha256':records['claim'],'canonical':envelope}
        if blobs['started'] is not None:
            started=parsed(blobs['started'])
            fields(started,(*binding,'phase','executor','request_sha256',*(['created_at'] if 'created_at' in claim else [])))
            expected=dict(binding,phase='launch_intent',executor=None,request_sha256=r['request_sha256'])
            if 'created_at' in claim:expected['created_at']=claim['created_at']
            if encoded(started)!=encoded(expected):raise ReceiptError('launch-intent binding mismatch')
            records['started']=sha(blobs['started'])
        if blobs['terminal'] is not None:
            if 'started' not in records or not r['executor']:raise ReceiptError('terminal lacks admitted launch binding')
            terminal=parsed(blobs['terminal'])
            fields(terminal,(*binding,'phase','executor','acceptance_verified','result',*(['finished_at'] if 'finished_at' in terminal else [])))
            expected=dict(binding,phase='execution_finished',executor=json.loads(r['executor']),acceptance_verified=False)
            if encoded({k:terminal[k] for k in expected})!=encoded(expected):raise ReceiptError('terminal identity or acceptance mismatch')
            if 'finished_at' in terminal:
                finished=timestamp(terminal['finished_at'])
                if start_time and finished<start_time:raise ReceiptError('terminal precedes launch intent')
            result=terminal['result'];required={'exit_code','timed_out','cleanup_complete','capture_complete','stdout_bytes','stderr_bytes','stdout_sha256','stderr_sha256'}
            if not isinstance(result,dict) or set(result) not in (required,required|{'duration_s'}):raise ReceiptError('invalid terminal result fields')
            if result['exit_code'] is not None and type(result['exit_code']) is not int:raise ReceiptError('invalid exit code')
            for k in ('timed_out','cleanup_complete','capture_complete'):
                if type(result[k]) is not bool:raise ReceiptError('invalid result boolean')
            for k in ('stdout_bytes','stderr_bytes'):
                if type(result[k]) is not int or not 0<=result[k]<=2**63-1:raise ReceiptError('invalid output count')
            for k in ('stdout_sha256','stderr_sha256'):hex64(result[k])
            if 'duration_s' in result and (type(result['duration_s']) not in (int,float) or not 0<=result['duration_s']<=172800 or not math.isfinite(result['duration_s'])):
                raise ReceiptError('invalid duration')
            records['terminal']=sha(blobs['terminal'])
        state='execution_succeeded_pending_review' if self._success(result) else 'effects_unknown'
        return {'state':state,'retryable':False,'records':records,'result':result}

    @staticmethod
    def _success(result):
        return bool(result is not None and result['exit_code']==0 and not result['timed_out'] and result['cleanup_complete'] and result['capture_complete'])

    def inspect_receipts(self, envelope, read):
        envelope=self._receipt_binding(envelope)
        r=self._attempt(envelope)
        if not r['admitted']:raise ReceiptError('attempt was not admitted')
        return self._read(envelope,read,r)

    @staticmethod
    def _receipt_binding(envelope):
        # The transport callback must never change the attempt being validated.
        try:
            return parsed(encoded(envelope))
        except (TypeError, ValueError, RecursionError):
            raise ReceiptError('invalid canonical envelope') from None

    def import_receipts(self, envelope, read):
        envelope=self._receipt_binding(envelope)
        observed=self.inspect_receipts(envelope,read)  # I/O outside transaction.
        with self.store._transaction():
            r=self._attempt(envelope)
            if r['state']=='accepted':raise ReceiptError('receipt replay after acceptance')
            new=False
            for phase,digest in observed['records'].items():
                old=r[phase+'_sha256']
                if old and old!=digest:raise ReceiptError('immutable receipt conflict')
                new |= old is None
            if not new:
                if observed['records']:raise ReceiptError('receipt replay')
                return {'state':'effects_unknown','retryable':False}
            # Any late new observation invalidates a prior effects-absent resolution.
            self.store.conn.execute("UPDATE aws_attempts SET claim_sha256=?,started_sha256=?,terminal_sha256=?,terminal_result=?,state=?,version=version+1 WHERE attempt_id=?",
                (observed['records'].get('claim'),observed['records'].get('started'),observed['records'].get('terminal'),
                 encoded(observed['result']).decode(),observed['state'],envelope['attempt_id']))
            self._event(envelope,'observed',envelope['dispatcher_id'],observed['records'].get('terminal','no terminal'))
            state=observed['state']
            if r['withdrawn']:state='withdrawn_effects_unknown'
            else:
                try:self._current(r,envelope)
                except ReceiptError:state='stale_claim_effects_unknown'
            return {'state':state,'retryable':False,'terminal_sha256':observed['records'].get('terminal')}

    def status(self, envelope):
        r=self._attempt(envelope)
        return {'state':r['state'],'withdrawn':bool(r['withdrawn']),'observations_sha256':sha(encoded(r)),
                'retryable':r['state']=='retry_authorized'}

    def reconcile_retry(self, envelope, actor, *, fence_evidence, effects_absent_evidence, observations_sha256=None):
        text(fence_evidence);text(effects_absent_evidence);hex64(observations_sha256)
        with self.store._transaction():
            r=self._attempt(envelope);self._independent(actor,envelope,json.loads(r['executor']) if r['executor'] else None)
            if not r['withdrawn'] or r['state'] in ('accepted','retry_authorized') or sha(encoded(r))!=observations_sha256:
                raise ReceiptError('withdrawal and current observed evidence required for reconciliation')
            self.store.conn.execute("UPDATE aws_attempts SET state='retry_authorized',version=version+1 WHERE attempt_id=?",(envelope['attempt_id'],))
            self._event(envelope,'effects-absent',actor,sha(encoded([fence_evidence,effects_absent_evidence,observations_sha256])))

    def accept(self, envelope, actor, *, terminal_sha256, evidence):
        hex64(terminal_sha256);text(evidence)
        with self.store._transaction():
            r=self._attempt(envelope);self._independent(actor,envelope,json.loads(r['executor']) if r['executor'] else None)
            t=self._current(r,envelope)
            result=json.loads(r['terminal_result']) if r['terminal_result'] else None
            if (r['withdrawn'] or r['state']!='execution_succeeded_pending_review'
                    or r['terminal_sha256']!=terminal_sha256 or not self._success(result)):
                raise ReceiptError('current independent terminal acceptance requirements not met')
            self.store.conn.execute("UPDATE aws_attempts SET state='accepted',version=version+1 WHERE attempt_id=?",(envelope['attempt_id'],))
            self.store.conn.execute("UPDATE aws_enrollments SET state='accepted' WHERE task_id=?",(envelope['task_id'],))
            self._event(envelope,'accepted',actor,sha(encoded([terminal_sha256,evidence])))
            t.receipts.append('independent AWS acceptance terminal sha256='+terminal_sha256)
            t.state='done';t.blocked_on=''
            return self.store._save(t,'done',actor,'independent AWS acceptance '+terminal_sha256)
