"""Tests for gateway/sticker_cache.py — sticker description cache."""

import asyncio
import importlib
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from gateway.sticker_cache import (
    _load_cache,
    _save_cache,
    get_cached_description,
    cache_sticker_description,
    build_sticker_injection,
    build_animated_sticker_injection,
)


class TestLoadSaveCache:
    def test_load_missing_file(self, tmp_path):
        with patch("gateway.sticker_cache.CACHE_PATH", tmp_path / "nope.json"):
            assert _load_cache() == {}

    def test_load_corrupt_file(self, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not json{{{")
        with patch("gateway.sticker_cache.CACHE_PATH", bad_file):
            assert _load_cache() == {}

    def test_save_and_load_roundtrip(self, tmp_path):
        cache_file = tmp_path / "cache.json"
        data = {"abc123": {"description": "A cat", "emoji": "", "set_name": "", "cached_at": 1.0}}
        with patch("gateway.sticker_cache.CACHE_PATH", cache_file):
            _save_cache(data)
            loaded = _load_cache()
        assert loaded == data

    def test_save_creates_parent_dirs(self, tmp_path):
        cache_file = tmp_path / "sub" / "dir" / "cache.json"
        with patch("gateway.sticker_cache.CACHE_PATH", cache_file):
            _save_cache({"key": "value"})
        assert cache_file.exists()


class TestCacheSticker:
    def test_cache_and_retrieve(self, tmp_path):
        cache_file = tmp_path / "cache.json"
        with patch("gateway.sticker_cache.CACHE_PATH", cache_file):
            cache_sticker_description("uid_1", "A happy dog", emoji="🐕", set_name="Dogs")
            result = get_cached_description("uid_1")

        assert result is not None
        assert result["description"] == "A happy dog"
        assert result["emoji"] == "🐕"
        assert result["set_name"] == "Dogs"
        assert "cached_at" in result

    def test_missing_sticker_returns_none(self, tmp_path):
        cache_file = tmp_path / "cache.json"
        with patch("gateway.sticker_cache.CACHE_PATH", cache_file):
            result = get_cached_description("nonexistent")
        assert result is None

    def test_overwrite_existing(self, tmp_path):
        cache_file = tmp_path / "cache.json"
        with patch("gateway.sticker_cache.CACHE_PATH", cache_file):
            cache_sticker_description("uid_1", "Old description")
            cache_sticker_description("uid_1", "New description")
            result = get_cached_description("uid_1")

        assert result["description"] == "New description"

    def test_multiple_stickers(self, tmp_path):
        cache_file = tmp_path / "cache.json"
        with patch("gateway.sticker_cache.CACHE_PATH", cache_file):
            cache_sticker_description("uid_1", "Cat")
            cache_sticker_description("uid_2", "Dog")
            r1 = get_cached_description("uid_1")
            r2 = get_cached_description("uid_2")

        assert r1["description"] == "Cat"
        assert r2["description"] == "Dog"


class TestBuildStickerInjection:
    def test_exact_format_no_context(self):
        result = build_sticker_injection("A cat waving")
        assert result == '[The user sent a sticker~ It shows: "A cat waving" (=^.w.^=)]'

    def test_exact_format_emoji_only(self):
        result = build_sticker_injection("A cat", emoji="😀")
        assert result == '[The user sent a sticker 😀~ It shows: "A cat" (=^.w.^=)]'

    def test_exact_format_emoji_and_set_name(self):
        result = build_sticker_injection("A cat", emoji="😀", set_name="MyPack")
        assert result == '[The user sent a sticker 😀 from "MyPack"~ It shows: "A cat" (=^.w.^=)]'

    def test_set_name_without_emoji_ignored(self):
        """set_name alone (no emoji) produces no context — only emoji+set_name triggers 'from' clause."""
        result = build_sticker_injection("A cat", set_name="MyPack")
        assert result == '[The user sent a sticker~ It shows: "A cat" (=^.w.^=)]'
        assert "MyPack" not in result

    def test_description_with_quotes(self):
        result = build_sticker_injection('A "happy" dog')
        assert '"A \\"happy\\" dog"' not in result  # no escaping happens
        assert 'A "happy" dog' in result

    def test_empty_description(self):
        result = build_sticker_injection("")
        assert result == '[The user sent a sticker~ It shows: "" (=^.w.^=)]'


class TestBuildAnimatedStickerInjection:
    def test_exact_format_with_emoji(self):
        result = build_animated_sticker_injection(emoji="🎉")
        assert result == (
            "[The user sent an animated sticker 🎉~ "
            "I can't see animated ones yet, but the emoji suggests: 🎉]"
        )

    def test_exact_format_without_emoji(self):
        result = build_animated_sticker_injection()
        assert result == "[The user sent an animated sticker~ I can't see animated ones yet]"

    def test_empty_emoji_same_as_no_emoji(self):
        result = build_animated_sticker_injection(emoji="")
        assert result == build_animated_sticker_injection()


class TestRestoredStickerIntegration:
    @pytest.fixture
    def adapter_module(self, monkeypatch, tmp_path):
        from tests.gateway._plugin_adapter_loader import load_plugin_adapter

        # Gateway conftest installs SDK mocks at collection time. Temporarily
        # remove only Telegram entries and the loader's cached adapter so this
        # fixture exercises an installed SDK, never that fallback mock.
        def owned_module(name):
            return (
                name == "plugin_adapter_telegram"
                or name == "telegram" or name.startswith("telegram.")
                or name == "plugins.platforms.telegram"
                or name.startswith("plugins.platforms.telegram.")
            )

        saved_modules = {name: module for name, module in sys.modules.items() if owned_module(name)}
        saved_path = list(sys.path)
        missing = object()
        parent_package = sys.modules.get("plugins.platforms")
        saved_parent_binding = getattr(parent_package, "telegram", missing)
        try:
            for name in saved_modules:
                sys.modules.pop(name, None)
            if parent_package is not None and hasattr(parent_package, "telegram"):
                delattr(parent_package, "telegram")
            telegram = importlib.import_module("telegram")
            assert isinstance(telegram, ModuleType) and not isinstance(telegram, Mock)
            assert isinstance(telegram.Bot, type)
            adapter = load_plugin_adapter("telegram")
            assert adapter.TELEGRAM_AVAILABLE and adapter.Bot is telegram.Bot
            monkeypatch.setattr("gateway.sticker_cache.CACHE_PATH", tmp_path / "stickers.json")
            # Tested branches must return before image caching/vision analysis.
            cache_image = Mock(side_effect=AssertionError("unexpected image/vision path"))
            monkeypatch.setattr(adapter, "cache_image_from_bytes", cache_image)
            yield adapter
            cache_image.assert_not_called()
        finally:
            for name in list(sys.modules):
                if owned_module(name):
                    sys.modules.pop(name, None)
            sys.modules.update(saved_modules)
            sys.path[:] = saved_path
            parent_package = sys.modules.get("plugins.platforms")
            if parent_package is not None:
                if saved_parent_binding is not missing:
                    parent_package.telegram = saved_parent_binding
                elif hasattr(parent_package, "telegram"):
                    delattr(parent_package, "telegram")

    @pytest.mark.parametrize("kind", ["animated", "video"])
    def test_nonstatic_sticker_imports_cache_without_downloading(self, adapter_module, kind):
        download = AsyncMock(side_effect=AssertionError("unexpected sticker download"))
        sticker = SimpleNamespace(
            is_animated=kind == "animated", is_video=kind == "video",
            emoji="🎉", set_name="Celebrations", get_file=download,
        )
        event = SimpleNamespace(text="")
        # This branch needs no connected adapter instance or bot constructor.
        asyncio.run(adapter_module.TelegramAdapter._handle_sticker(
            None, SimpleNamespace(sticker=sticker), event,
        ))
        assert event.text == build_animated_sticker_injection("🎉")
        download.assert_not_called()

    def test_static_sticker_uses_real_persisted_cache(self, adapter_module):
        cache_sticker_description("static-1", "A waving cat", "🐈", "Cats")
        download = AsyncMock(side_effect=AssertionError("unexpected sticker download"))
        sticker = SimpleNamespace(
            is_animated=False, is_video=False, file_unique_id="static-1",
            emoji="🐈", set_name="Cats", get_file=download,
        )
        event = SimpleNamespace(text="")
        asyncio.run(adapter_module.TelegramAdapter._handle_sticker(
            None, SimpleNamespace(sticker=sticker), event,
        ))
        assert event.text == build_sticker_injection("A waving cat", "🐈", "Cats")
        assert get_cached_description("static-1")["description"] == "A waving cat"
        download.assert_not_called()

    def test_failed_atomic_replace_preserves_previous_cache(self, tmp_path):
        cache_file = tmp_path / "stickers.json"
        with patch("gateway.sticker_cache.CACHE_PATH", cache_file):
            _save_cache({"existing": {"description": "Keep this"}})
            previous_bytes = cache_file.read_bytes()
            with patch("gateway.sticker_cache.os.replace", side_effect=OSError("replace failed")):
                with pytest.raises(OSError, match="replace failed"):
                    _save_cache({"replacement": {"description": "Not saved"}})
            assert cache_file.read_bytes() == previous_bytes
            assert _load_cache() == {"existing": {"description": "Keep this"}}
            assert list(tmp_path.glob("*.tmp")) == []
