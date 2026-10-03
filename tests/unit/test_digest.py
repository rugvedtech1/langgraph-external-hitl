from langgraph_external_hitl.digest import payload_digest


def test_digest_is_canonical():
    assert payload_digest({"a": 1, "b": 2}) == payload_digest({"b": 2, "a": 1})
    assert payload_digest({"action": "x"}) != payload_digest({"action": "y"})
    assert len(payload_digest({"action": "x"})) == 64
