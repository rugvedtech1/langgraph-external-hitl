"""Build wheel + sdist from a COPY of the source tree with planted secrets and NO
.gitignore, then verify nothing sensitive is packaged. Skipped when the source
tree or the `build` tool is unavailable."""
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

pytest.importorskip("build")


@pytest.fixture(scope="module")
def dists(tmp_path_factory, request):
    root = Path(__file__).resolve().parents[2]
    if not (root / "pyproject.toml").is_file() or not (root / "src").is_dir():
        pytest.skip("source tree not available")
    work = tmp_path_factory.mktemp("proj") / "p"
    shutil.copytree(root, work, ignore=shutil.ignore_patterns(".git", ".venv", "dist", "__pycache__",
                                                                ".pytest_cache", "*.db", ".env"))
    (work / ".gitignore").unlink(missing_ok=True)                   # worst case: no .gitignore
    (work / ".env").write_text("TELEGRAM_BOT_TOKEN=planted-secret\n")
    (work / "approvals.db").write_bytes(b"planted")
    (work / "tests" / "leak.db").write_bytes(b"planted")
    (work / "examples" / ".env").write_text("x\n")
    out = work / "dist"
    r = subprocess.run([sys.executable, "-m", "build", "--outdir", str(out), str(work)],
                       capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stdout + r.stderr
    wheel = next(out.glob("*.whl"))
    sdist = next(out.glob("*.tar.gz"))
    return wheel, sdist


def _names(wheel, sdist):
    with zipfile.ZipFile(wheel) as z:
        w = z.namelist()
    with tarfile.open(sdist) as t:
        s = t.getnames()
    return w, s


def test_no_secrets_in_wheel_or_sdist(dists):
    w, s = _names(*dists)
    for name in w + s:
        base = name.rsplit("/", 1)[-1]
        assert base != ".env" and not base.startswith(".env."), name
        assert not base.endswith(".db"), name


def test_wheel_contains_only_the_package(dists):
    w, _ = _names(*dists)
    assert "langgraph_external_hitl/py.typed" in w
    assert "langgraph_external_hitl/telegram/_handlers.py" in w
    assert not any(n.startswith(("tests/", "examples/", "src/")) for n in w), w
    assert any(n.endswith("licenses/LICENSE") for n in w)


def test_sdist_contains_source_tests_examples(dists):
    _, s = _names(*dists)
    assert any(n.endswith("src/langgraph_external_hitl/__init__.py") for n in s)
    assert any("/tests/" in n for n in s) and any("/examples/quickstart.py" in n for n in s)


def test_twine_check(dists):
    pytest.importorskip("twine")
    r = subprocess.run([sys.executable, "-m", "twine", "check", "--strict", *map(str, dists)],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout + r.stderr
