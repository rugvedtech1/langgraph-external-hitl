from importlib.metadata import metadata, version

import langgraph_external_hitl


def test_version_single_source():
    assert langgraph_external_hitl.__version__ == version("langgraph-external-hitl") == "0.6.0"


def test_core_metadata():
    md = metadata("langgraph-external-hitl")
    assert md["Requires-Python"] == ">=3.12"
    assert md["License-Expression"] == "MIT"
    extras = set(md.get_all("Provides-Extra"))
    assert {"telegram", "langgraph", "all"} <= extras
    reqs = md.get_all("Requires-Dist") or []
    assert all("extra ==" in r for r in reqs), reqs   # core has no unconditional deps
