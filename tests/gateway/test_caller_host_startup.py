"""Real gateway construction/start with an inert platform and isolated profile."""
import builtins
import pytest

from gateway.config import GatewayConfig, load_gateway_config
from gateway.run import GatewayRunner
from gateway.caller_activation import HostError


@pytest.mark.parametrize('value', [None, False, 'true', 1])
def test_only_explicit_yaml_boolean_enables(value):
    config = GatewayConfig.from_dict({'gateway': {'caller_host_enabled': value}})
    assert config.caller_host_enabled is False


def test_nested_profile_config_loads_and_roundtrips(monkeypatch, tmp_path):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    (tmp_path / 'config.yaml').write_text('gateway:\n  caller_host_enabled: true\n  caller_host_provider: trusted_host:build\n')
    config = load_gateway_config()
    assert config.caller_host_enabled is True
    assert config.caller_host_provider == 'trusted_host:build'
    assert GatewayConfig.from_dict(config.to_dict()).caller_host_enabled is True


@pytest.mark.asyncio
@pytest.mark.parametrize('explicit_off', [False, True])
async def test_flag_off_gateway_boot_does_not_load_host(monkeypatch, tmp_path, explicit_off):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    native_import = builtins.__import__
    def guarded_import(name, *args, **kwargs):
        if name in {'gateway.caller_activation', 'gateway.cloud_caller', 'gateway.host_adapters'}:
            raise AssertionError('off gateway imported host')
        return native_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', guarded_import)
    kwargs = {'caller_host_enabled': False} if explicit_off else {}
    config = GatewayConfig(sessions_dir=tmp_path / 'sessions',
                           caller_host_provider='must_not_import:build', **kwargs)
    runner = GatewayRunner(config, caller_host_services=object())
    assert runner._context_tool_factory is None
    try:
        assert await runner.start() is True
        assert runner._context_tool_factory is None
    finally:
        await runner.stop()


def test_flag_on_missing_provider_fails_before_gateway_boot(monkeypatch, tmp_path):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    with pytest.raises(HostError, match='caller_host_provider_required'):
        GatewayRunner(GatewayConfig(caller_host_enabled=True))
