"""Probes for the compat shim that runs on Hermes runtimes WITHOUT the shared scoped readers.

Hermes 0.20.x ships ``agent.secret_scope`` (per-profile secret scope, fail-closed) but not
``gateway/platforms/_shared.py``. The adapter's compatibility shim must still resolve a
secondary profile's settings from ITS scope — the first version of the fix fell straight back to
``os.environ`` there, which is the LAUNCHER's (default profile's) environment under
``gateway.multiplex_profiles``, i.e. the original cross-profile leak.

This module deliberately imports neither ``gateway.platforms._shared`` nor anything that needs
it, so it runs (and reports real results, not a skip) on both runtime generations; it forces the
shim by poisoning the module in ``sys.modules`` before reloading the adapter.
"""

import contextlib
import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import adapter


try:  # the runtime floor for the shim's second rung
    from agent.secret_scope import get_secret as _scope_get_secret  # noqa: F401
    from gateway.run import _profile_runtime_scope  # noqa: F401
except ImportError as _exc:  # pragma: no cover - a runtime with no secret scope at all
    raise unittest.SkipTest(f"this runtime has no per-profile secret scope: {_exc}")


def _ensure_registered() -> None:
    """``Platform("nextcloud_talk")`` resolves only for a registered platform (same registration
    the other suites make; ``register`` is last-writer-wins, so the gate is this module's)."""
    from gateway.platform_registry import PlatformEntry, platform_registry

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


LAUNCHER_ENV = {
    "NEXTCLOUD_TALK_URL": "https://launcher.example",
    "NEXTCLOUD_TALK_USERNAME": "launcher-bot",
    "NEXTCLOUD_TALK_PASSWORD": "launcher-secret",
    "NEXTCLOUD_TALK_ROOM_TOKENS": "launcherroom",
    "NEXTCLOUD_TALK_ALLOWED_USERS": "ivan",
}

SECONDARY_ENV = {
    "NEXTCLOUD_TALK_URL": "http://secondary.example:11000",
    "NEXTCLOUD_TALK_USERNAME": "secondary-bot",
    "NEXTCLOUD_TALK_PASSWORD": "secondary-secret",
    "NEXTCLOUD_TALK_ROOM_TOKENS": "ownroom",
    "NEXTCLOUD_TALK_ALLOWED_USERS": "ivan,Ivan",
    "NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP": "true",
}


@contextlib.contextmanager
def _launcher_env():
    """``os.environ`` as the launcher sees it, with the host's own ``NEXTCLOUD_TALK*`` keys dropped.

    Importing the adapter pulls in the core modules that hydrate the active profile's env file into
    the process environment, so a live host hands this probe real Talk settings for keys the fixture
    does not name (the singular ``NEXTCLOUD_TALK_ROOM_TOKEN``, say). Confining the environment to the
    fixture's own keys is what keeps the assertions independent of where the suite runs.
    """
    keys = {key for key in os.environ if key.startswith("NEXTCLOUD_TALK")} | set(LAUNCHER_ENV) | set(SECONDARY_ENV)
    saved = {key: os.environ.get(key) for key in keys}
    for key in keys:
        os.environ.pop(key, None)
    os.environ.update(LAUNCHER_ENV)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextlib.contextmanager
def _multiplex(active=True):
    from agent.secret_scope import is_multiplex_active, set_multiplex_active

    previous = is_multiplex_active()
    set_multiplex_active(active)
    try:
        yield
    finally:
        set_multiplex_active(previous)


@contextlib.contextmanager
def _shimmed_adapter():
    """Reload the adapter with the shared readers unavailable, i.e. the older runtime's branch."""
    try:
        with patch.dict(sys.modules, {"gateway.platforms._shared": None}):
            yield importlib.reload(adapter)
    finally:
        importlib.reload(adapter)  # outside the patch: the other modules see the real one


def _write_env(home: Path, values: dict) -> None:
    home.mkdir(parents=True, exist_ok=True)
    (home / ".env").write_text("".join(f"{k}={v}\n" for k, v in values.items()), encoding="utf-8")


class CompatShimTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.profile_home = Path(self._tmp.name) / "profiles" / "secondary"
        _write_env(self.profile_home, SECONDARY_ENV)

    def test_shim_is_scope_aware_for_a_secondary_profile(self):
        with _launcher_env(), _multiplex(), _shimmed_adapter() as shimmed:
            with _profile_runtime_scope(self.profile_home):
                instance = shimmed.NextcloudTalkAdapter(_platform_config())
            self.assertEqual(instance.base_url, "http://secondary.example:11000")
            self.assertEqual(instance.username, "secondary-bot")
            self.assertEqual(instance.password, "secondary-secret")
            self.assertEqual(instance.room_tokens, ["ownroom"])
            self.assertEqual(instance.allowed_users, {"ivan", "Ivan"})
            self.assertFalse(instance.allow_all)
            self.assertNotIn("launcher-bot", (instance.username, instance.password, instance.base_url))
            self.assertNotIn("launcherroom", instance.room_tokens)

    def test_shim_fails_closed_without_the_profile_credentials(self):
        from agent.secret_scope import build_profile_secret_scope

        empty_home = Path(self._tmp.name) / "profiles" / "bare"
        _write_env(empty_home, {"DEEPSEEK_API_KEY": "x" * 10})
        with _launcher_env(), _multiplex(), _shimmed_adapter() as shimmed:
            self.assertTrue(build_profile_secret_scope(empty_home))
            with _profile_runtime_scope(empty_home):
                instance = shimmed.NextcloudTalkAdapter(_platform_config())
                self.assertEqual(instance.username, "")
                self.assertEqual(instance.room_tokens, [])
                self.assertEqual(instance.allowed_users, set())
                self.assertFalse(shimmed.validate_config(_platform_config()))

    def test_shim_keeps_the_unscoped_default_lane_on_os_environ(self):
        with _launcher_env(), _multiplex(), _shimmed_adapter() as shimmed:
            instance = shimmed.NextcloudTalkAdapter(_platform_config())
            self.assertEqual(instance.username, "launcher-bot")
            self.assertEqual(instance.base_url, "https://launcher.example")
            self.assertEqual(instance.room_tokens, ["launcherroom"])

    def test_shim_replaced_the_shared_readers_and_was_restored(self):
        """The reload probes above must leave the REAL core readers bound for whatever runs next."""
        with _launcher_env(), _multiplex(False), _shimmed_adapter() as shimmed:
            self.assertTrue(callable(shimmed._scoped_env))
            self.assertTrue(callable(shimmed._extra_or_secret))
            self.assertTrue(callable(shimmed._gate_env))
        shared = sys.modules.get("gateway.platforms._shared")
        if shared is not None:  # modern runtime: the real readers are bound again afterwards
            self.assertIs(adapter._scoped_env, shared.get_scoped_secret)
            self.assertIs(adapter._extra_or_secret, shared.extra_or_secret)
            self.assertIs(adapter._gate_env, shared.platform_gate_env)


def _platform_config():
    from gateway.config import PlatformConfig

    return PlatformConfig(enabled=True, typing_indicator=False, extra={})


def tearDownModule():
    """Leave the process as the neighbouring suites expect it: the real core readers bound (this
    module reloads the adapter with the shim several times). The platform registration is left in
    place on purpose — the sibling suites and the core's authorization path resolve
    `allowed_users_env`/`allow_all_env` through it, exactly as plugin discovery does in a real
    gateway, and removing it made those suites fail in a reverse-ordered run. The readers are
    captured BEFORE the reload below, which re-binds the real ones and would otherwise make the
    assertion hold no matter what the probes left."""
    bound = (adapter._scoped_env, adapter._extra_or_secret, adapter._gate_env)
    importlib.reload(adapter)
    shared = sys.modules.get("gateway.platforms._shared")
    if shared is not None:
        assert bound[0] is shared.get_scoped_secret, "shim left bound after the probes"
        assert bound[1] is shared.extra_or_secret, "shim left bound after the probes"
        assert bound[2] is shared.platform_gate_env, "shim left bound after the probes"


if __name__ == "__main__":
    unittest.main()
