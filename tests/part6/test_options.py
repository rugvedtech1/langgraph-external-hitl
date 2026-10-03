"""Part 6: option model, validation, rendering, keyboard and callback parsing."""
import pytest

from langgraph_external_hitl import APPROVE_REJECT, MAX_OPTIONS, ActionTooLongError, ApprovalOption, ApprovalStore, InvalidOptionsError
from langgraph_external_hitl.options import validate_options, validate_request
from langgraph_external_hitl.bridge import build_payload
from langgraph_external_hitl.telegram._handlers import keyboard, parse_option_callback, render

O = ApprovalOption


def opts(n):
    return [O(f"opt{i}", f"Option {i}") for i in range(n)]


@pytest.mark.parametrize("n", [1, 2, 3, 5, 10])
def test_valid_counts(n):
    assert len(validate_options(opts(n))) == n


@pytest.mark.parametrize("n", [0, 11, 20])
def test_invalid_counts(n):
    with pytest.raises(InvalidOptionsError):
        validate_options(opts(n))


@pytest.mark.parametrize("bad", [
    [O("a", "A"), O("a", "B")],                      # duplicate id
    [O("a", "Same"), O("b", "same ")],               # duplicate label (case/space-insensitive)
    [O("bad id", "A")], [O("", "A")], [O("x" * 65, "A")], [O("é", "A")],
    [O("a", "")], [O("a", "   ")], [O("a", "two\nlines")], [O("a", "L" * 65)],
    [O("a", "A", description="d" * 201)], [O("a", "A", style="purple")],  # type: ignore[arg-type]
    ["approve"],                                       # not an ApprovalOption
])
def test_invalid_options(bad):
    with pytest.raises(InvalidOptionsError):
        validate_options(bad)


def test_total_length_cap_and_title_required():
    with pytest.raises(ActionTooLongError):
        validate_request("t", "m" * 3500, APPROVE_REJECT)
    with pytest.raises(ActionTooLongError):
        validate_request("  ", None, APPROVE_REJECT)
    validate_request("t" * 100, "m" * 3000, APPROVE_REJECT)


def test_build_payload_legacy_and_v2():
    assert build_payload("Deploy?") == {"action": "Deploy?"}
    p = build_payload("Deploy", "How?", [O("prod", "Prod", style="danger")])
    assert p == {"v": 2, "title": "Deploy", "message": "How?",
                 "options": [{"id": "prod", "label": "Prod", "description": None, "style": "danger"}]}
    with pytest.raises(InvalidOptionsError):
        build_payload("Deploy", "How?", [])


def _approval(tmp_path, options, title="Deploy <b>now</b>", message="Pick & go"):
    s = ApprovalStore(str(tmp_path / "a.db"))
    return s, s.create(1, 1, title, now=0, ttl_s=60, message=message, options=options)


def test_render_escapes_html_and_lists_descriptions(tmp_path):
    s, a = _approval(tmp_path, [O("x", "<script>", description="a & b"), O("y", "Plain")])
    text = render(a)
    assert "<b>Deploy &lt;b&gt;now&lt;/b&gt;</b>" in text and "Pick &amp; go" in text
    assert "&lt;script&gt;" in text and "a &amp; b" in text and "<script>" not in text
    s.close()


@pytest.mark.parametrize("n,rows", [(1, 1), (2, 1), (3, 1), (4, 4), (10, 10)])
def test_keyboard_layout_and_opaque_callback_data(tmp_path, n, rows):
    s, a = _approval(tmp_path, opts(n))
    kb = keyboard(a)["inline_keyboard"]
    assert len(kb) == rows
    flat = [b for row in kb for b in row]
    assert [b["text"] for b in flat] == [f"Option {i}" for i in range(n)]
    for i, b in enumerate(flat):
        assert b["callback_data"] == f"v2:{a.approval_id}:{i}"
        assert len(b["callback_data"].encode()) <= 64 and "opt" not in b["callback_data"]
    s.close()


def test_keyboard_passes_style(tmp_path):
    s, a = _approval(tmp_path, list(APPROVE_REJECT))
    assert [b.get("style") for b in keyboard(a)["inline_keyboard"][0]] == ["success", "danger"]
    s.close()


def test_parse_option_callback():
    assert parse_option_callback("v2:abc_-9:0") == ("abc_-9", 0)
    assert parse_option_callback("v2:abc:9") == ("abc", 9)
    assert parse_option_callback("a:abc") == ("abc", "approve")
    assert parse_option_callback("r:abc") == ("abc", "reject")
    for bad in ["", "v2:abc", "v2:abc:100", "v2:abc:-1", "v2:ab c:1", "v2:abc:1:2", "v3:abc:1",
                "v2::1", "x:abc", "v2:" + "a" * 44 + ":1"]:
        assert parse_option_callback(bad) is None, bad
