"""The GPU watchdog's decision table.

The interesting part is not the HTTP plumbing but *which of three outcomes*
each ollama state maps to — restart, re-pin, or do nothing. Getting that wrong
is expensive in opposite directions: a missed CPU fallback silently costs 3
orders of magnitude of latency (and trips extraction's liveness probe), while a
spurious restart kills in-flight inference for no reason.
"""

from __future__ import annotations

import pytest

from docpipeline import config
from docpipeline.reconciliation import gpu_watchdog


def _gpu(name: str) -> dict:
    return {"name": name, "size_vram": 1_400_000_000}


def _cpu(name: str) -> dict:
    # The signature of the bug: ollama loaded the model and is serving happily,
    # just with nothing offloaded to the GPU.
    return {"name": name, "size_vram": 0}


class TestClassify:
    def test_splits_on_vram_residency(self):
        on_gpu, on_cpu = gpu_watchdog.classify([_gpu("llama3.2:1b"), _cpu("qwen2.5:1.5b")])
        assert on_gpu == {"llama3.2:1b"}
        assert on_cpu == {"qwen2.5:1.5b"}

    def test_nothing_loaded_is_neither(self):
        # The idle-unload case — not a fault, and must not be read as one.
        assert gpu_watchdog.classify([]) == (set(), set())

    def test_falls_back_to_model_key(self):
        # /api/ps has used both `name` and `model` across ollama versions.
        on_gpu, _ = gpu_watchdog.classify([{"model": "llama3.2:1b", "size_vram": 1}])
        assert on_gpu == {"llama3.2:1b"}


class TestCheckOnce:
    @pytest.fixture(autouse=True)
    def _calls(self, monkeypatch):
        calls = {"restart": 0, "pinned": []}
        monkeypatch.setattr(gpu_watchdog, "restart_ollama", lambda: calls.__setitem__("restart", calls["restart"] + 1))
        monkeypatch.setattr(gpu_watchdog, "pin", lambda m: calls["pinned"].append(m))
        monkeypatch.setattr(config, "OLLAMA_MODELS", ["llama3.2:1b", "qwen2.5:1.5b"])
        monkeypatch.setattr(config, "GPU_WATCHDOG_DRY_RUN", False)
        return calls

    def _with_models(self, monkeypatch, models):
        monkeypatch.setattr(gpu_watchdog, "loaded_models", lambda: models)

    def test_all_on_gpu_does_nothing(self, monkeypatch, _calls):
        self._with_models(monkeypatch, [_gpu("llama3.2:1b"), _gpu("qwen2.5:1.5b")])
        gpu_watchdog.check_once(0.0)
        assert _calls == {"restart": 0, "pinned": []}

    def test_cpu_resident_restarts(self, monkeypatch, _calls):
        self._with_models(monkeypatch, [_cpu("llama3.2:1b")])
        assert gpu_watchdog.check_once(0.0) > 0  # records the restart time
        assert _calls["restart"] == 1

    def test_cpu_resident_respects_cooldown(self, monkeypatch, _calls):
        import time
        self._with_models(monkeypatch, [_cpu("llama3.2:1b")])
        just_now = time.time()
        assert gpu_watchdog.check_once(just_now) == just_now  # unchanged
        assert _calls["restart"] == 0, "a second restart inside the cooldown would thrash the pod"

    def test_dry_run_never_restarts(self, monkeypatch, _calls):
        monkeypatch.setattr(config, "GPU_WATCHDOG_DRY_RUN", True)
        self._with_models(monkeypatch, [_cpu("llama3.2:1b")])
        gpu_watchdog.check_once(0.0)
        assert _calls["restart"] == 0

    def test_unloaded_model_is_repinned_not_restarted(self, monkeypatch, _calls):
        # The 5-minute idle unload. Re-pin; restarting here would be pure harm.
        self._with_models(monkeypatch, [_gpu("llama3.2:1b")])
        gpu_watchdog.check_once(0.0)
        assert _calls["restart"] == 0
        assert _calls["pinned"] == ["qwen2.5:1.5b"]

    def test_nothing_loaded_pins_every_tier(self, monkeypatch, _calls):
        self._with_models(monkeypatch, [])
        gpu_watchdog.check_once(0.0)
        assert _calls["restart"] == 0
        assert _calls["pinned"] == ["llama3.2:1b", "qwen2.5:1.5b"]

    def test_cpu_fallback_wins_over_missing(self, monkeypatch, _calls):
        # One model on CPU, the other not loaded at all: restart, don't pin —
        # pinning onto a broken pod just loads the second model onto CPU too.
        self._with_models(monkeypatch, [_cpu("llama3.2:1b")])
        gpu_watchdog.check_once(0.0)
        assert _calls["restart"] == 1
        assert _calls["pinned"] == []

    def test_unreachable_ollama_is_not_a_gpu_fault(self, monkeypatch, _calls):
        def _boom():
            raise gpu_watchdog.OllamaUnreachable("connection refused")
        monkeypatch.setattr(gpu_watchdog, "loaded_models", _boom)
        gpu_watchdog.check_once(0.0)
        # Ollama restarting is not evidence the GPU broke; restarting it again
        # would turn a transient blip into a loop.
        assert _calls == {"restart": 0, "pinned": []}
