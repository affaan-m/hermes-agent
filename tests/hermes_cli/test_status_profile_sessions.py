"""Actual status handler/SessionDB regressions with isolated SQLite state.

No AST extraction, application startup, provider calls or production fixtures.
The repository conftest establishes import-time HERMES_HOME isolation.
"""
import asyncio
import hashlib
import sqlite3
import time
from types import SimpleNamespace

import pytest

NOW = 2_000_000_000.0


@pytest.fixture
def status_context(tmp_path, monkeypatch, _isolate_hermes_home):
    from hermes_cli import profiles
    from hermes_constants import get_hermes_home, set_hermes_home_override, reset_hermes_home_override
    import hermes_cli.web_server as server
    import hermes_cli.auth as auth

    default = get_hermes_home()
    homes = {'default': default, 'desk': default/'profiles/desk', 'buyer': default/'profiles/buyer'}
    for home in homes.values():
        home.mkdir(parents=True, exist_ok=True)
        (home/'config.yaml').write_text('{}\n')
    monkeypatch.setattr(profiles, '_get_default_hermes_home', lambda: default)
    monkeypatch.setattr(profiles, '_get_profiles_root', lambda: default/'profiles')
    token = set_hermes_home_override(homes['desk'])
    monkeypatch.setattr(server, 'time', SimpleNamespace(time=lambda: NOW, monotonic=time.monotonic))
    monkeypatch.setattr(server, 'check_config_version', lambda: (1, 1))
    monkeypatch.setattr(server, '_dashboard_local_update_managed_externally', lambda: False)
    monkeypatch.setattr(server, '_load_configured_gateway_platforms', lambda: {'telegram'})
    monkeypatch.setattr(server, '_resolve_restart_drain_timeout', lambda: 30)
    monkeypatch.setattr(server, 'get_install_id', lambda: 'a'*32)
    monkeypatch.setattr(auth, 'get_nous_session_validity', lambda: 'unknown')
    monkeypatch.setattr(server.app.state, 'auth_required', True, raising=False)
    topology = {'profiles':list(homes), 'gateway_mode':'multiple','gateways':[],
                'profile_platforms':{'buyer':{'telegram':{'state':'fatal','writer_pid':999}}}}
    monkeypatch.setattr(server, '_collect_profile_gateway_topology_cached', lambda: topology)
    monkeypatch.setattr(server, 'read_runtime_status', lambda *a, **k: None)
    monkeypatch.setattr(server, 'get_running_pid_cached', lambda *a, **k: None)
    monkeypatch.setattr(server, 'get_runtime_status_running_pid', lambda *a, **k: None)
    monkeypatch.setattr(server, '_GATEWAY_HEALTH_URL', 'http://synthetic.invalid')
    probes = []
    def health():
        probes.append('dashboard')
        return True, {'gateway_state':'running','pid':123,'updated_at':1_700_000_000.0,
                      'platforms':{'telegram':{'state':'connected','writer_pid':123},
                                   'unsafe:bad@key':{'state':'connected'}}}
    monkeypatch.setattr(server, '_probe_gateway_health', health)
    try:
        yield SimpleNamespace(server=server, homes=homes, probes=probes)
    finally:
        reset_hermes_home_override(token)


def make_db(home, rows):
    from hermes_state import SessionDB
    path = home/'state.db'
    db = SessionDB(db_path=path)
    try:
        for key, _, _, _ in rows:
            db.create_session(key, 'cli')
    finally:
        db.close()
    with sqlite3.connect(path) as connection:
        for key, started, active, ended in rows:
            connection.execute('UPDATE sessions SET started_at=?, last_activity_at=?, ended_at=? WHERE id=?',
                               (started, active, ended, key))
    return path


def test_selected_profile_count_survives_executor_and_preserves_gateway_scope(status_context, monkeypatch):
    c = status_context
    make_db(c.homes['desk'], [('desk',NOW-50,NOW-5,None)])
    make_db(c.homes['buyer'], [('buyer1',NOW-50,NOW-5,None),('buyer2',NOW-40,NOW-4,None)])
    seen = []
    def pid(path=None):
        seen.append(path)
        return 456 if path == c.homes['buyer']/'gateway.pid' else None
    monkeypatch.setattr(c.server,'get_running_pid_cached',pid)
    result = asyncio.run(c.server.get_status('buyer'))
    assert result['gateway_profile'] == 'buyer'
    assert result['active_sessions'] == 2
    assert result['active_sessions_available'] is True
    assert (result['active_sessions_window_seconds'],result['active_sessions_limit']) == (300,50)
    assert seen == [c.homes['buyer']/'gateway.pid']
    assert c.probes == []
    assert result['auth_required'] is True
    assert 'gateway_pid' not in result and 'hermes_home' not in result
    assert result['profiles'] == ['default','desk','buyer']


def test_current_default_and_named_dashboard_remote_health_boundary(status_context):
    c = status_context
    for requested,expected,remote in [('current','desk',True),('desk','desk',True),('default','default',False),('buyer','buyer',False)]:
        result = asyncio.run(c.server.get_status(requested))
        assert result['gateway_profile'] == expected
        assert result['gateway_running'] is remote
        assert result['gateway_platforms'] == ({'telegram':{'state':'connected'}} if remote else {})
        if remote:
            assert isinstance(result['gateway_updated_at'],str)
        assert result['active_sessions'] is None
        assert result['active_sessions_available'] is False
    assert c.probes == ['dashboard','dashboard']


def test_parallel_profiles_keep_counts_and_context_separate(status_context):
    c = status_context
    make_db(c.homes['desk'], [('desk',NOW-1,NOW-1,None)])
    make_db(c.homes['buyer'], [])
    async def poll():
        return await asyncio.gather(c.server.get_status('current'),c.server.get_status('buyer'))
    desk,buyer = asyncio.run(poll())
    assert (desk['gateway_profile'],desk['active_sessions']) == ('desk',1)
    assert (buyer['gateway_profile'],buyer['active_sessions']) == ('buyer',0)
    from hermes_constants import get_hermes_home
    assert get_hermes_home() == c.homes['desk']


def test_recent_unended_sample_is_read_only_and_excludes_future(status_context, monkeypatch):
    c = status_context
    rows = [('recent',NOW-100,NOW-1,None),('old',NOW-1000,NOW-301,None),
            ('boundary',NOW-1000,NOW-300,None),('future',NOW-5,NOW+1,None),
            ('ended',NOW-100,NOW-1,NOW-1)]
    path = make_db(c.homes['buyer'],rows)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    from hermes_state import SessionDB
    original = SessionDB.list_sessions_rich
    calls=[]
    def inspect_read(db,*args,**kwargs):
        calls.append((db.read_only,db.db_path,kwargs))
        return original(db,*args,**kwargs)
    monkeypatch.setattr(SessionDB,'list_sessions_rich',inspect_read)
    result = asyncio.run(c.server._status_active_sessions(path))
    assert result == 1
    assert calls == [(True,path,{'limit':50,'compact_rows':True})]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_newest_start_sample_remains_bounded_to_fifty(status_context):
    c = status_context
    rows = [('recent-old-root',NOW-10000,NOW-1,None)]
    rows += [(f'newer-{i}',NOW-1000+i,NOW-1000+i,None) for i in range(50)]
    path=make_db(c.homes['buyer'],rows)
    assert asyncio.run(c.server._status_active_sessions(path)) == 0


def test_missing_corrupt_and_stale_schema_are_unknown_without_healing(status_context):
    c = status_context
    path=c.homes['buyer']/'state.db'
    assert asyncio.run(c.server._status_active_sessions(path)) is None
    assert not path.exists()
    path.write_bytes(b'not sqlite')
    before=path.read_bytes()
    assert asyncio.run(c.server._status_active_sessions(path)) is None
    assert path.read_bytes() == before
    path.unlink()
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE sessions (id TEXT PRIMARY KEY)')
    before=hashlib.sha256(path.read_bytes()).hexdigest()
    assert asyncio.run(c.server._status_active_sessions(path)) is None
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_count_timeout_is_unknown_and_not_zero(status_context,monkeypatch):
    c=status_context
    monkeypatch.setattr(c.server,'_STATUS_ACTIVE_SESSIONS_TIMEOUT',.001)
    def slow(*args):
        time.sleep(.02)
        return 4
    monkeypatch.setattr(c.server,'_count_status_active_sessions',slow)
    assert asyncio.run(c.server._status_active_sessions(c.homes['buyer']/'state.db')) is None


def test_plain_machine_rollup_and_invalid_profile_behavior_preserved(status_context):
    c=status_context
    result=asyncio.run(c.server.get_status())
    assert result['gateway_profile']=='desk'
    assert result['gateway_mode']=='multiple'
    assert result['gateway_platforms']['buyer:telegram']=={'state':'fatal'}
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as error:
        asyncio.run(c.server.get_status('../escape'))
    assert error.value.status_code==400
