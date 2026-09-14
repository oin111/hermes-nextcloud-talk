"""Multiplex profile-scope probes for the Nextcloud Talk adapter.

Contract under test: under ``gateway.multiplex_profiles`` one gateway process serves several
profiles, and a SECONDARY profile's adapter is built inside that profile's secret scope.
Every credential / room-token / allowlist read must therefore resolve from the owning
profile's own ``.env``.

Regression this file pins down: the adapter read its settings with a raw ``os.getenv``, so a
secondary lane inherited the LAUNCHER's values — the default profile's bot credentials and
room list — and answered in the default profile's rooms under the default profile's account.
The launcher's values are what ``os.environ`` holds in this process; the secondary's live only
in its scope.
"""

import contextlib
import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent.secret_scope import (
    build_profile_secret_scope,
    set_multiplex_active,
    set_secret_scope,
    reset_secret_scope,
)
from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry

import adapter


try:
    # The profile secret scope itself plus the readers the adapter imports. A runtime whose
    # gateway/platforms/_shared.py lacks extra_or_secret (or lacks the module) skips here instead
    # of failing: the adapter's compat shims keep it working (OlderRuntimeFallbackTests,
    # test_compat_shim.py), it just reaches isolation through the shim.
    from gateway.platforms._shared import extra_or_secret as _core_extra_or_secret  # noqa: F401
    from gateway.run import _profile_runtime_scope  # noqa: F401
except ImportError as _exc:  # pragma: no cover - exercised on the legacy-runtime CI job
    raise unittest.SkipTest(f"profile secret-scope API unavailable in this Hermes runtime: {_exc}")


# ``Platform("nextcloud_talk")`` only resolves once the platform is known to the registry.
# Mirrors test_lifecycle_real.py's global test registration (same pre-construction load order).
# ``is_registered`` also answers for a deferred loader in another scope, so check for a CONCRETE
# entry: with a temp profile home active, a deferred-only registration would leave
# ``Platform("nextcloud_talk")`` unresolvable and error the whole module.
def _ensure_registered() -> None:
    """Register under the CURRENT scope, the way per-profile plugin discovery does (a profile
    scope has its own entry map, so a construction through the registry needs it there too).

    The entry carries THIS module's ``validate_config``/``is_connected``: without them the core's
    ``create_adapter`` skips the configuration gate, and a foreign entry (an installed plugin, or
    another test module) would answer the probes instead of the module under test. ``register``
    is last-writer-wins, so an entry without our gate is replaced."""
    entry = platform_registry.get("nextcloud_talk")
    if entry is not None and entry.validate_config is adapter.validate_config:
        return
    platform_registry.register(
        PlatformEntry(
            name="nextcloud_talk",
            label="Nextcloud Talk",
            adapter_factory=lambda cfg: adapter.NextcloudTalkAdapter(cfg),
            check_fn=lambda: True,
            validate_config=adapter.validate_config,
            is_connected=adapter.is_connected,
            source="builtin",
            allowed_users_env="NEXTCLOUD_TALK_ALLOWED_USERS",
            allow_all_env="NEXTCLOUD_TALK_ALLOW_ALL_USERS",
        )
    )


_ensure_registered()

# The registry probes must exercise THIS module's gate, not whatever entry another actor
# registered for the real platform name (an installed plugin copy, or another test module in the
# same process). A dedicated probe name makes that deterministic.
_PROBE_PLATFORM = "nextcloud_talk_scope_probe"


def _ensure_probe_entry() -> None:
    platform_registry.register(
        PlatformEntry(
            name=_PROBE_PLATFORM,
            label="Nextcloud Talk (scope probe)",
            adapter_factory=lambda cfg: adapter.NextcloudTalkAdapter(cfg),
            check_fn=lambda: True,
            validate_config=adapter.validate_config,
            is_connected=adapter.is_connected,
            source="builtin",
            allowed_users_env="NEXTCLOUD_TALK_ALLOWED_USERS",
            allow_all_env="NEXTCLOUD_TALK_ALLOW_ALL_USERS",
        )
    )


# The DEFAULT profile (first writer into os.environ) — different server, bot, rooms, allowlist.
LAUNCHER_ENV = {
    "NEXTCLOUD_TALK_URL": "https://default.example",
    "NEXTCLOUD_TALK_USERNAME": "default-bot",
    "NEXTCLOUD_TALK_PASSWORD": "default-secret",
    "NEXTCLOUD_TALK_ROOM_TOKENS": "defaultroom1,defaultroom2",
    "NEXTCLOUD_TALK_ALLOWED_USERS": "ivan",
    "NEXTCLOUD_TALK_ALLOW_ALL_USERS": "false",
    "NEXTCLOUD_TALK_MAX_MESSAGE_LENGTH": "4096",
}

# A SECONDARY profile's own .env (shape of a per-profile Talk lane: its own server, bot, room).
SECONDARY_ENV = {
    "NEXTCLOUD_TALK_URL": "http://talk.example:11000",
    "NEXTCLOUD_TALK_USERNAME": "secondary-bot",
    "NEXTCLOUD_TALK_PASSWORD": "secondary-secret",
    "NEXTCLOUD_TALK_ROOM_TOKEN": "legacy-own-room",
    "NEXTCLOUD_TALK_ROOM_TOKENS": "ownroom",
    "NEXTCLOUD_TALK_ALLOWED_USERS": "ivan,Ivan",
    "NEXTCLOUD_TALK_ALLOW_ALL_USERS": "false",
    "NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP": "true",
    "NEXTCLOUD_TALK_MAX_MESSAGE_LENGTH": "2048",
}


def _write_env(profile_home: Path, values: dict) -> None:
    profile_home.mkdir(parents=True, exist_ok=True)
    (profile_home / ".env").write_text(
        "".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8"
    )


@contextlib.contextmanager
def _launcher_env():
    """``os.environ`` as the multiplexer's launch process sees it: the DEFAULT profile's values."""
    saved = {key: os.environ.get(key) for key in set(LAUNCHER_ENV) | set(SECONDARY_ENV)}
    os.environ.update(LAUNCHER_ENV)
    # The secondary's own values are NOT in the launcher's environment — that is the whole point:
    # keys that only the secondary's .env defines are simply absent here.
    for key in SECONDARY_ENV:
        if key not in LAUNCHER_ENV:
            os.environ.pop(key, None)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextlib.contextmanager
def _multiplex(active: bool = True):
    from agent.secret_scope import is_multiplex_active

    previous = is_multiplex_active()
    set_multiplex_active(active)
    try:
        yield
    finally:
        set_multiplex_active(previous)


@contextlib.contextmanager
def _secondary_profile_scope(profile_home: Path):
    """Enter a secondary profile exactly the way the multiplexer builds its adapters."""
    from gateway.run import _profile_runtime_scope

    with _profile_runtime_scope(Path(profile_home)):
        yield


def _make_adapter(extra=None):
    return adapter.NextcloudTalkAdapter(PlatformConfig(enabled=True, typing_indicator=False, extra=extra or {}))


class SecondaryProfileIsolationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.profile_home = Path(self._tmp.name) / "profiles" / "secondary"
        _write_env(self.profile_home, SECONDARY_ENV)

    def test_secondary_profile_reads_its_own_credentials_rooms_and_allowlist(self):
        """A secondary lane must connect as ITS OWN bot, to ITS OWN rooms, with ITS OWN allowlist."""
        with _launcher_env(), _multiplex(), _secondary_profile_scope(self.profile_home):
            instance = _make_adapter()

            self.assertEqual(instance.base_url, "http://talk.example:11000")
            self.assertEqual(instance.username, "secondary-bot")
            self.assertEqual(instance.password, "secondary-secret")
            self.assertEqual(set(instance.room_tokens), {"ownroom", "legacy-own-room"})
            self.assertEqual(instance.allowed_users, {"ivan", "Ivan"})
            self.assertEqual(instance.group_allowed_users, {"ivan", "Ivan"})
            self.assertFalse(instance.allow_all)
            self.assertEqual(instance.max_message_length, 2048)

            # The launcher's identity must not appear anywhere in the secondary lane.
            self.assertNotIn("default-bot", (instance.username, instance.password, instance.base_url))
            self.assertNotIn("defaultroom1", instance.room_tokens)
            self.assertNotIn("defaultroom2", instance.room_tokens)
            self.assertEqual(instance.allowed_users, {"ivan", "Ivan"},
                             "the launcher's single-user allowlist must not be inherited")

    def test_secondary_profile_cursor_lives_in_its_own_home(self):
        with _launcher_env(), _multiplex(), _secondary_profile_scope(self.profile_home):
            instance = _make_adapter()
            self.assertTrue(
                str(instance._cursor_path).startswith(str(self.profile_home)),
                f"cursor path escaped the profile home: {instance._cursor_path}",
            )

    def test_secondary_profile_never_reads_raw_env_for_its_settings(self):
        """Anti-regression: any raw env read (``os.getenv`` or a direct ``os.environ`` lookup) on a
        NEXTCLOUD_TALK_* var breaks isolation again."""
        raw_reads: list[str] = []
        real_getenv = os.getenv
        real_environ = os.environ

        def spy_getenv(name, default=None):
            if isinstance(name, str) and name.startswith("NEXTCLOUD_TALK"):
                raw_reads.append(name)
            return real_getenv(name, default)

        class _RecordingEnviron(dict):
            def get(self, name, default=None):
                if isinstance(name, str) and name.startswith("NEXTCLOUD_TALK"):
                    raw_reads.append(name)
                return real_environ.get(name, default)

            def __getitem__(self, name):
                if isinstance(name, str) and name.startswith("NEXTCLOUD_TALK"):
                    raw_reads.append(name)
                return real_environ[name]

            def __contains__(self, name):
                return name in real_environ

        with _launcher_env(), _multiplex(), _secondary_profile_scope(self.profile_home):
            with patch.object(os, "getenv", spy_getenv), patch.object(os, "environ", _RecordingEnviron()):
                instance = _make_adapter()

        self.assertEqual(raw_reads, [], f"raw env reads under a secondary scope: {sorted(set(raw_reads))}")
        self.assertEqual(instance.username, "secondary-bot")

    def test_secondary_profile_http_url_opt_in_comes_from_its_own_env(self):
        """``NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP`` is a per-profile gate, not a launcher one."""
        with _launcher_env(), _multiplex(), _secondary_profile_scope(self.profile_home):
            self.assertTrue(adapter.validate_config(_config_for_validation()))
            # The client refuses a plain-HTTP base URL unless THIS profile opted in.
            client = adapter.NextcloudTalkClient("http://talk.example:11000", "secondary-bot", "s")
            self.assertEqual(client.base_url, "http://talk.example:11000")

    def test_registry_factory_builds_the_lane_from_the_profile_scope(self):
        """The real construction path (platform_registry.create_adapter) resolves the secondary's own
        settings — that factory consults ``validate_config``, so it is also the fail-closed gate."""
        with _launcher_env(), _multiplex(), _secondary_profile_scope(self.profile_home):
            _ensure_registered()  # the factory needs Platform("nextcloud_talk") resolvable
            _ensure_probe_entry()
            entry = platform_registry.get(_PROBE_PLATFORM)
            self.assertIs(entry.validate_config, adapter.validate_config,
                          "the probe must exercise THIS module's configuration gate")
            instance = platform_registry.create_adapter(_PROBE_PLATFORM, _config_for_validation())
            self.assertIsNotNone(instance)
            self.assertEqual(instance.username, "secondary-bot")
            self.assertEqual(set(instance.room_tokens), {"ownroom", "legacy-own-room"})

    def test_registry_factory_refuses_an_uncredentialed_secondary_lane(self):
        """The lane's own missing credentials must stop construction at the registry, not silently
        fall back to the launcher's bot."""
        from agent.secret_scope import set_secret_scope

        token = set_secret_scope({"DEEPSEEK_API_KEY": "x" * 10})
        try:
            with _launcher_env(), _multiplex():
                _ensure_registered()
                _ensure_probe_entry()
                entry = platform_registry.get(_PROBE_PLATFORM)
                self.assertIs(entry.validate_config, adapter.validate_config,
                              "the probe must exercise THIS module's configuration gate")
                self.assertIsNone(
                    platform_registry.create_adapter(_PROBE_PLATFORM, _config_for_validation())
                )
        finally:
            reset_secret_scope(token)

    def test_uncredentialed_secondary_profile_fails_closed(self):
        """No Talk credentials of its own: the lane must stay unconfigured, never borrow the launcher's."""
        from agent.secret_scope import set_secret_scope

        token = set_secret_scope({"DEEPSEEK_API_KEY": "x" * 10})
        try:
            with _launcher_env(), _multiplex():
                instance = _make_adapter()
                self.assertEqual(instance.base_url, "")
                self.assertEqual(instance.username, "")
                self.assertEqual(instance.password, "")
                self.assertEqual(instance.room_tokens, [])
                self.assertEqual(instance.allowed_users, set())
                self.assertFalse(instance.allow_all)
                config = _config_for_validation()
                self.assertFalse(adapter.validate_config(config))
                self.assertFalse(adapter.is_connected(config))
        finally:
            reset_secret_scope(token)

    def test_secondary_scope_credentials_validate(self):
        with _launcher_env(), _multiplex(), _secondary_profile_scope(self.profile_home):
            self.assertTrue(adapter.validate_config(_config_for_validation()))
            self.assertTrue(adapter.is_connected(_config_for_validation()))


class NonSecondaryProfilesKeepEnvSemanticsTests(unittest.TestCase):
    """The default profile (unscoped) and single-profile installs must keep reading os.environ."""

    def test_default_profile_unscoped_under_multiplexer_reads_os_environ(self):
        with _launcher_env(), _multiplex():
            instance = _make_adapter()
            self.assertEqual(instance.base_url, "https://default.example")
            self.assertEqual(instance.username, "default-bot")
            self.assertEqual(instance.room_tokens, ["defaultroom1", "defaultroom2"])
            self.assertEqual(instance.allowed_users, {"ivan"})
            self.assertEqual(instance.max_message_length, 4096)

    def test_single_profile_install_reads_os_environ(self):
        with _launcher_env(), _multiplex(False):
            instance = _make_adapter()
            self.assertEqual(instance.base_url, "https://default.example")
            self.assertEqual(instance.username, "default-bot")

    def test_yaml_extra_rung_still_works_when_env_is_absent(self):
        """The YAML ``extra`` rung stays: no env value -> the profile's own config.yaml wins."""
        with _launcher_env(), _multiplex(False):
            saved = {key: os.environ.pop(key, None)
                     for key in ("NEXTCLOUD_TALK_USERNAME", "NEXTCLOUD_TALK_ROOM_TOKENS",
                                 "NEXTCLOUD_TALK_REQUIRE_MENTION")}
            try:
                instance = _make_adapter({"username": "yaml-bot", "room_tokens": "yamlroom",
                                          "require_mention": True})
                self.assertEqual(instance.username, "yaml-bot")
                self.assertEqual(instance.room_tokens, ["yamlroom"])
                self.assertTrue(instance.require_mention)
            finally:
                for key, value in saved.items():
                    if value is not None:
                        os.environ[key] = value

    def test_explicit_env_still_beats_yaml_for_the_owning_profile(self):
        """Env-over-YAML stays the documented contract for the profile that owns the value."""
        with _launcher_env(), _multiplex(False):
            instance = _make_adapter({"username": "yaml-bot"})
            self.assertEqual(instance.username, "default-bot")

    def test_blank_env_value_is_unset(self):
        """Pins the core rule the conversion adopts: a blank env value is UNSET, so the profile's own
        YAML applies (the pre-fix reader treated a present-but-blank env var as a hard False/'')."""
        with _launcher_env(), _multiplex(False):
            saved = {key: os.environ.get(key) for key in
                     ("NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS", "NEXTCLOUD_TALK_REQUIRE_MENTION",
                      "NEXTCLOUD_TALK_UPLOAD_FOLDER")}
            try:
                os.environ["NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS"] = ""
                os.environ["NEXTCLOUD_TALK_REQUIRE_MENTION"] = "   "
                os.environ["NEXTCLOUD_TALK_UPLOAD_FOLDER"] = ""
                # No YAML value: the blank env falls through to the setting's default.
                self.assertTrue(_make_adapter().auto_discover_rooms)
                self.assertEqual(_make_adapter().upload_folder, "/Hermes Uploads")
                # With a YAML value, that value applies instead of the pre-fix hard False.
                self.assertTrue(_make_adapter({"require_mention": True}).require_mention)
                self.assertEqual(_make_adapter({"upload_folder": ""}).upload_folder, "/Hermes Uploads")
            finally:
                for key, value in saved.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value


class ScopeWithoutSharedReadersTests(unittest.TestCase):
    """Hermes 0.20.x ships ``agent.secret_scope`` but not ``gateway/platforms/_shared.py``:
    the compat shim must inline the core reader so a secondary lane is STILL isolated there."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.profile_home = Path(self._tmp.name) / "profiles" / "secondary"
        _write_env(self.profile_home, SECONDARY_ENV)

    def test_secondary_lane_is_still_isolated_without_the_shared_readers(self):
        from gateway.platforms import _shared

        try:
            with _launcher_env(), _multiplex(), patch.dict(
                sys.modules, {"gateway.platforms._shared": None}
            ):
                legacy = importlib.reload(adapter)
                self.assertIsNot(legacy._scoped_env, _shared.get_scoped_secret)
                token = set_secret_scope(build_profile_secret_scope(self.profile_home))
                try:
                    instance = legacy.NextcloudTalkAdapter(
                        PlatformConfig(enabled=True, typing_indicator=False, extra={})
                    )
                finally:
                    reset_secret_scope(token)
                self.assertEqual(instance.username, "secondary-bot")
                self.assertEqual(instance.base_url, "http://talk.example:11000")
                self.assertEqual(set(instance.room_tokens), {"ownroom", "legacy-own-room"})
                self.assertEqual(instance.allowed_users, {"ivan", "Ivan"})
                self.assertFalse(instance.allow_all)
        finally:
            importlib.reload(adapter)

    def test_uncredentialed_secondary_lane_fails_closed_without_the_shared_readers(self):
        from agent.secret_scope import set_secret_scope as install_scope

        try:
            with _launcher_env(), _multiplex(), patch.dict(
                sys.modules, {"gateway.platforms._shared": None}
            ):
                legacy = importlib.reload(adapter)
                token = install_scope({"DEEPSEEK_API_KEY": "x" * 10})
                try:
                    instance = legacy.NextcloudTalkAdapter(
                        PlatformConfig(enabled=True, typing_indicator=False, extra={})
                    )
                    self.assertEqual(instance.username, "")
                    self.assertEqual(instance.room_tokens, [])
                    self.assertFalse(legacy.validate_config(_config_for_validation()))
                finally:
                    reset_secret_scope(token)
        finally:
            importlib.reload(adapter)


class OlderRuntimeFallbackTests(unittest.TestCase):
    """Runtimes without the shared scope-aware readers keep the legacy env behavior."""

    def test_adapter_still_reads_env_when_the_shared_reader_is_absent(self):
        from gateway.platforms import _shared

        try:
            # ``sys.modules[name] = None`` makes the import raise, i.e. an older Hermes runtime.
            with _launcher_env(), _multiplex(False), patch.dict(
                sys.modules, {"gateway.platforms._shared": None}
            ):
                legacy = importlib.reload(adapter)
                # The compat shim replaced the core reader instead of importing it.
                self.assertIsNot(legacy._scoped_env, _shared.get_scoped_secret)
                instance = legacy.NextcloudTalkAdapter(
                    PlatformConfig(enabled=True, typing_indicator=False, extra={})
                )
                self.assertEqual(instance.username, "default-bot")
                self.assertEqual(instance.base_url, "https://default.example")
                self.assertEqual(instance.room_tokens, ["defaultroom1", "defaultroom2"])
                self.assertEqual(instance.allowed_users, {"ivan"})
        finally:
            # Outside the patch, so the real-core module is the one the other tests see.
            importlib.reload(adapter)


def _config_for_validation():
    return PlatformConfig(enabled=True, extra={})


def tearDownModule():
    """Leave the process as the neighbouring suites expect it.

    The restored-reader check lives here, not in a test method: unittest runs classes
    alphabetically, so ``OlderRuntimeFallbackTests`` (which swaps the readers out) executes BEFORE
    ``ScopeWithoutSharedReadersTests``, and a same-module test asserting restoration would not
    guard the mutation it names. The readers are captured BEFORE the reload below, which re-binds
    the real ones and would otherwise make the assertion hold no matter what the probes left.
    Also drop the probe entry this module registered, so a later module or a real run in the same
    process sees the registry it expects.
    """
    bound = (adapter._scoped_env, adapter._extra_or_secret, adapter._gate_env)
    importlib.reload(adapter)
    shared = sys.modules.get("gateway.platforms._shared")
    if shared is not None:
        assert bound[0] is shared.get_scoped_secret, "shim left bound after the probes"
        assert bound[1] is shared.extra_or_secret, "shim left bound after the probes"
        assert bound[2] is shared.platform_gate_env, "shim left bound after the probes"
    # Only the probe entry is dropped: the real platform name stays registered because the sibling
    # suites (and the core's authorization path) resolve `allowed_users_env`/`allow_all_env`
    # through it, and plugin discovery registers the same name in a real gateway.
    with contextlib.suppress(Exception):
        platform_registry.unregister(_PROBE_PLATFORM)


if __name__ == "__main__":
    unittest.main()
