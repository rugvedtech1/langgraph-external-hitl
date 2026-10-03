"""Optional-extras behaviour, checked in fresh subprocesses."""
import subprocess
import sys
import textwrap


def run_py(code: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-c", textwrap.dedent(code)],
                          capture_output=True, text=True, timeout=60)


def test_core_import_loads_no_optional_dependencies():
    r = run_py("""
        import sys, langgraph_external_hitl
        bad = [m for m in ("httpx", "langgraph", "aiosqlite", "langchain_core") if m in sys.modules]
        print("LOADED:", bad)
    """)
    assert r.returncode == 0, r.stderr
    assert "LOADED: []" in r.stdout


def test_telegram_without_httpx_gives_clear_error():
    r = run_py("""
        import sys
        sys.modules["httpx"] = None  # simulate: extra not installed
        from langgraph_external_hitl import MissingDependencyError
        try:
            import langgraph_external_hitl.telegram
        except MissingDependencyError as e:
            assert isinstance(e, ImportError)
            print("OK:", e)
    """)
    assert r.returncode == 0, r.stderr
    assert "pip install 'langgraph-external-hitl[telegram]'" in r.stdout


def test_bridge_without_langgraph_gives_clear_error():
    r = run_py("""
        import sys
        sys.modules["langgraph"] = None
        sys.modules["langgraph.types"] = None
        from langgraph_external_hitl import MissingDependencyError
        try:
            import langgraph_external_hitl.bridge
        except MissingDependencyError as e:
            print("OK:", e)
    """)
    assert r.returncode == 0, r.stderr
    assert "pip install 'langgraph-external-hitl[langgraph]'" in r.stdout


def test_telegram_import_does_not_require_langgraph():
    r = run_py("""
        import sys
        sys.modules["langgraph"] = None
        sys.modules["langgraph.types"] = None
        import langgraph_external_hitl.telegram as t
        print("OK", t.TelegramApprovalBot.__name__)
    """)
    assert r.returncode == 0, r.stderr
    assert "OK TelegramApprovalBot" in r.stdout
