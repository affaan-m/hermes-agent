"""Private typed-context consumer. Network/auth/canonical ports are host-owned.

No default transport, credential discovery, process execution or outward send.
Ports must authenticate artifact reads and atomically fence context consumption.
"""
from datetime import datetime
import hashlib
import json
import math
import re


MAX_BYTES = 8192
_BINDING = {'request_id', 'request_spec_sha256', 'manifest_sha256',
            'canonical_sha256', 'spec_sha256', 'input_sha256', 'source_sha256', 'executor'}
_CONTEXT = {'status', 'generated_at', 'source', 'rows', 'truncated', 'scope', 'identity_authority'}
_RESULT = {'exit_code', 'timed_out', 'cleanup_complete', 'capture_complete',
           'stdout_bytes', 'stderr_bytes', 'stdout_sha256', 'stderr_sha256', 'duration_s'}


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('utf-8')


def _copy(value):
    return json.loads(_encoded(value))


def _hex(value, count):
    return type(value) is str and re.fullmatch('[0-9a-f]{' + str(count) + '}', value) is not None


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate field')
        result[key] = value
    return result


def _finite(text):
    value = float(text)
    if not math.isfinite(value):
        raise ValueError('nonfinite value')
    return value


def _invalid_constant(_text):
    raise ValueError('nonfinite value')


def _validate_binding(binding):
    if type(binding) is not dict or set(binding) != _BINDING:
        raise ValueError('binding fields')
    if not _hex(binding['request_id'], 32):
        raise ValueError('request identity')
    if any(not _hex(binding[k], 64) for k in _BINDING - {'request_id', 'executor'}):
        raise ValueError('binding digest')
    executor = binding['executor']
    if type(executor) is not dict or set(executor) != {'task_arn', 'task_definition_arn', 'launch_token'}:
        raise ValueError('executor fields')
    if not _hex(executor['launch_token'], 32):
        raise ValueError('executor token')
    for key in ('task_arn', 'task_definition_arn'):
        if type(executor[key]) is not str or not 1 <= len(executor[key]) <= 512:
            raise ValueError('executor identity')
    # Exact ECS identity/image verification belongs to the trusted host observer.


def _validate_summary(summary, expected):
    if type(summary) is not dict or set(summary) != {'schema', 'canonical_sha256', 'spec_sha256', 'result', 'acceptance_verified'}:
        raise ValueError('summary fields')
    if type(summary['schema']) is not int or summary['schema'] != 2 or summary['acceptance_verified'] is not False:
        raise ValueError('summary schema')
    if any(summary[key] != expected[key] for key in ('canonical_sha256', 'spec_sha256')):
        raise ValueError('summary binding')
    result = summary['result']
    if type(result) is not dict or set(result) != _RESULT:
        raise ValueError('result fields')
    if type(result['exit_code']) is not int or result['exit_code'] != 0:
        raise ValueError('unsuccessful process')
    if result['timed_out'] is not False or result['cleanup_complete'] is not True or result['capture_complete'] is not True:
        raise ValueError('incomplete process')
    for key in ('stdout_bytes', 'stderr_bytes'):
        if type(result[key]) is not int or not 0 <= result[key] <= 2**63 - 1:
            raise ValueError('invalid byte count')
    if not 1 <= result['stdout_bytes'] <= MAX_BYTES:
        raise ValueError('context size')
    if any(not _hex(result[k], 64) for k in ('stdout_sha256', 'stderr_sha256')):
        raise ValueError('output digest')
    if type(result['duration_s']) not in (int, float) or not math.isfinite(result['duration_s']) or not 0 <= result['duration_s'] <= 172800:
        raise ValueError('duration')
    return result


def _decode(stdout, row_limit):
    if type(stdout) is not bytes or not 1 <= len(stdout) <= MAX_BYTES:
        raise ValueError('bounded output')
    context = json.loads(stdout.decode('utf-8'), object_pairs_hook=_unique,
                         parse_float=_finite, parse_constant=_invalid_constant)
    if type(context) is not dict or set(context) != _CONTEXT:
        raise ValueError('context fields')
    if context['status'] != 'ok' or context['scope'] != 'operator_projection' or context['identity_authority'] is not False:
        raise ValueError('projection status')
    _timestamp(context['generated_at'])
    if type(context['truncated']) is not bool or type(context['rows']) is not list or len(context['rows']) > row_limit:
        raise ValueError('projection bounds')
    for row in context['rows']:
        _row(row)
    ids = [row['inventory_id'] for row in context['rows']]
    if len(ids) != len(set(ids)):
        raise ValueError('duplicate rows')
    source = context['source']
    if type(source) is not dict or set(source) != {'coverage', 'observation', 'newest_event_at', 'event_age_seconds'}:
        raise ValueError('source fields')
    if source['coverage'] != 'unknown' or source['observation'] not in ('canonical_book_only', 'event_store_only'):
        raise ValueError('source assertion')
    if source['newest_event_at'] is not None:
        _timestamp(source['newest_event_at'])
    age = source['event_age_seconds']
    if age is not None and (type(age) is not int or not 0 <= age <= 2**53):
        raise ValueError('source age')
    return context

# Closed projection schema copied from pinned planner0793551a, not model fields.
_CELLS = {'gpu.exact_sku','gpu.memory_per_gpu_gb','gpu.interconnect_type',
    'gpu.network_bandwidth','gpu.cpu_ram_storage_specification','quantity.amount',
    'quantity.unit','quantity.gpus_per_node','delivery.transaction_type',
    'delivery.region','delivery.available_from','delivery.required_by','delivery.duration_months'}
_DIMS = {'sku','quantity','region','when','term','delivery'}
_CONFLICTS = _CELLS | {'constraints.'+k for k in _DIMS} | {
    'constraints.'+k+'_is_hard' for k in ('quantity','region','when','term')}
_SKUS = {'B300','B200','GB300','GB200','H200','H100','A100','A6000','L40S','L40','L4','V100','MI300X','MI325X','MI350X','unknown'}
_REGIONS = {'US','US East','US West','UK','EU','Canada','APAC','unknown',''}
_ROW = {'inventory_id','version','side','sku','quantity','unit','region','date','status','missing_cells','requirements','conflicts'}
_SPEC = {'schema_version','request_id','operation','input_sha256','input_bytes',
         'cli_source_manifest_sha256','audience_grant_sha256','projection','deadline_at','result_max_bytes'}

def _timestamp(value):
    if type(value) is not str or not 1 <= len(value) <= 40:
        raise ValueError('timestamp')
    stamp = datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError('unbound timestamp')

def _enum(value, allowed):
    if type(value) is not str or value not in allowed:
        raise ValueError('enum')

def _cells(values, allowed):
    if type(values) is not list or len(values) > len(allowed):
        raise ValueError('cell list')
    if any(type(v) is not str or v not in allowed for v in values) or len(values) != len(set(values)):
        raise ValueError('cell values')

def _row(row):
    if type(row) is not dict or set(row) != _ROW:
        raise ValueError('row fields')
    if type(row['inventory_id']) is not str or re.fullmatch('[A-Za-z0-9_.:-]{1,128}',row['inventory_id']) is None:
        raise ValueError('row identity')
    if type(row['version']) is not int or row['version'] < 1:
        raise ValueError('row version')
    _enum(row['side'], {'HAS','NEEDS'})
    _enum(row['sku'], _SKUS)
    _enum(row['unit'], {'gpus','nodes','racks','unknown'})
    _enum(row['region'], _REGIONS)
    _enum(row['status'], {'unconfirmed','available','reserved','withdrawn','expired','fulfilled'})
    if type(row['quantity']) is not int or not 0 <= row['quantity'] <= 1_000_000:
        raise ValueError('row quantity')
    date = row['date']
    if type(date) is not str or (date and re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}',date) is None):
        raise ValueError('row date')
    if date:
        datetime.strptime(date,'%Y-%m-%d')
    _cells(row['missing_cells'],_CELLS)
    _cells(row['conflicts'],_CONFLICTS)
    requirements = row['requirements']
    if type(requirements) is not dict or set(requirements) != (_DIMS if row['side']=='NEEDS' else set()):
        raise ValueError('row requirements')
    for value in requirements.values():
        _enum(value, {'hard','soft','unspecified'})

def _validate_spec(spec, binding):
    if type(spec) is not dict or set(spec) != _SPEC:
        raise ValueError('spec fields')
    if type(spec['schema_version']) is not int or spec['schema_version'] != 1 or spec['operation'] != 'nonpricing.context':
        raise ValueError('spec operation')
    if hashlib.sha256(_encoded(spec)).hexdigest() != binding['request_spec_sha256']:
        raise ValueError('spec commitment')
    if spec['request_id'] != binding['request_id'] or spec['input_sha256'] != binding['input_sha256'] or spec['cli_source_manifest_sha256'] != binding['source_sha256']:
        raise ValueError('spec binding')
    if not _hex(spec['audience_grant_sha256'],64):
        raise ValueError('grant binding')
    if type(spec['input_bytes']) is not int or not 1 <= spec['input_bytes'] <= 8388608:
        raise ValueError('input bound')
    if type(spec['result_max_bytes']) is not int or not 1 <= spec['result_max_bytes'] <= MAX_BYTES:
        raise ValueError('output bound')
    projection = spec['projection']
    if type(projection) is not dict or set(projection) != {'company_id','evaluation_time','limit'}:
        raise ValueError('projection fields')
    if type(projection['company_id']) is not str or re.fullmatch('[A-Za-z0-9_.:-]{1,128}',projection['company_id']) is None:
        raise ValueError('company filter')
    if type(projection['limit']) is not int or not 1 <= projection['limit'] <= 20:
        raise ValueError('row limit')
    for value in (projection['evaluation_time'],spec['deadline_at']):
        if type(value) not in (int,float) or not math.isfinite(value) or value < 0:
            raise ValueError('spec clock')
    # Parseability and operator --now are not proof of freshness/current authority.
    return spec['result_max_bytes'],projection['limit']


class ContextResults:
    def __init__(self, *, exchange, context_store):
        self.exchange = exchange
        self.context_store = context_store

    def consume(self, expected, summary, *, request_spec):
        """Consume only through a host-injected atomic current-fence commit port.

        `expected` and `summary` must come from trusted host observation, never
        directly from model input. The exchange must authenticate and bound its
        read before allocation. This validator cannot implement those guarantees.
        The context store must atomically validate current authority/sequence and
        perform its one-use context commit. A boolean flag in a model request is
        not an implementation of that port. Neither port has a default backend.
        """
        try:
            binding = _copy(expected)
            _validate_binding(binding)
            limit, row_limit = _validate_spec(_copy(request_spec), binding)
            result = _validate_summary(_copy(summary), binding)
            if result['stdout_bytes'] > limit:
                raise ValueError('request output limit')
            artifact = self.exchange.read_result(_copy(binding), max_bytes=limit)
            if type(artifact) is not dict or set(artifact) != {'binding', 'stdout'}:
                raise ValueError('artifact fields')
            _validate_binding(artifact['binding'])
            # Serialize to avoid bool/int equality accepting nonidentical bindings.
            if _encoded(artifact['binding']) != _encoded(binding):
                raise ValueError('artifact binding')
            stdout = artifact['stdout']
            if type(stdout) is not bytes or len(stdout) != result['stdout_bytes'] or hashlib.sha256(stdout).hexdigest() != result['stdout_sha256']:
                raise ValueError('artifact output mismatch')
            context = _decode(stdout, row_limit)
        except Exception:
            return {'status': 'result_unavailable', 'context_committed': False}
        try:
            committed = self.context_store.commit_context_once(
                _copy(binding), result['stdout_sha256'], context)
        except Exception:
            # It may have committed. No retry, fallback or returned context.
            return {'status': 'commit_unknown', 'context_committed': False}
        if committed is not True:
            return {'status': 'context_withheld', 'context_committed': False}
        return {'status': 'context_committed', 'context_committed': True,
                'request_id': binding['request_id'], 'result_sha256': result['stdout_sha256']}
