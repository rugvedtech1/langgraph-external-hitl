"""Run the shipped channel contract suite against FakeChannel and TelegramAdapter."""
import asyncio

import pytest
from langgraph.checkpoint.memory import InMemorySaver

import importlib.util as _ilu
import pathlib as _pl
_spec = _ilu.spec_from_file_location("_hitl_test_graphs", _pl.Path(__file__).resolve().parents[1] / "conftest.py")
_mod = _ilu.module_from_spec(_spec); _spec.loader.exec_module(_mod)
build_test_graph = _mod.build_test_graph
from langgraph_external_hitl import ApprovalStore, ConnectionManager, HitlService, register_secret
from langgraph_external_hitl.bridge import HitlBridge
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig, TelegramError
from langgraph_external_hitl.telegram._handlers import TOASTS
from langgraph_external_hitl.testing import CONTRACT_CHECKS, FakeChannel

NOW = 1_000_000


class _Base:
    def __init__(self, tmp_path):
        self.clock = {"t": NOW}
        self.results: dict = {}
        self.store = ApprovalStore(str(tmp_path / "hitl.db"))
        self.graph = build_test_graph(InMemorySaver(), lambda action, tid: None)
        self.bridge = HitlBridge(self.graph, self.store,
                                 on_completed=lambda tid, v: self.results.__setitem__(tid, v.get("result")))

    async def start_job(self, thread_id, recipient):
        await self.service.track(thread_id, recipient)
        await self.graph.ainvoke({"action": f"do {thread_id}"}, {"configurable": {"thread_id": thread_id}},
                                 durability="sync")
        return await self.service.deliver_pending(thread_id)

    def graph_result(self, thread_id):
        return self.results.get(thread_id)


class FakeHarness(_Base):
    secret = "fake-channel-secret-0123456789abcdef"

    def __init__(self, tmp_path):
        super().__init__(tmp_path)
        register_secret(self.secret)
        self.fake = FakeChannel()
        self.service = HitlService(self.bridge, self.fake, clock=lambda: self.clock["t"])

    def address_for(self, actor_ref):
        return {"inbox": actor_ref}

    async def interact(self, approval, choice, *, actor_ref, delivery_ref=None):
        report = await self.service.handle(self.fake.request(approval, choice, actor_ref=actor_ref,
                                                             delivery_ref=delivery_ref))
        return report.outcome

    def sent_count(self):
        return len(self.fake.sent)

    def fail_next_send(self, *, permanent, reason):
        self.fake.fail_next_send(permanent=permanent, reason=reason)

    def break_updates(self):
        self.fake.fail_updates = True


class _Api:
    def __init__(self):
        self.calls, self.mid, self.once, self.always = [], 500, {}, {}

    async def call(self, method, **p):
        self.calls.append((method, p))
        if method in self.always:
            raise self.always[method]()
        if method in self.once:
            raise self.once.pop(method)()
        if method == "sendMessage":
            self.mid += 1
            return {"message_id": self.mid}
        return True


_TOAST_PREFIX = {k: v.split("{")[0] for k, v in TOASTS.items()}


class TelegramHarness(_Base):
    secret = "123456789:AAH-CONTRACT-SECRET-TOKEN-abcdefghijk"

    def __init__(self, tmp_path):
        super().__init__(tmp_path)
        self.api = _Api()
        self.bot = TelegramApprovalBot(TelegramConfig(token=self.secret), self.bridge, api=self.api,
                                       clock=lambda: self.clock["t"], connections=ConnectionManager(self.store, "bot"))
        self.service = self.bot.service

    def address_for(self, actor_ref):
        return {"chat_id": int(actor_ref)}

    async def interact(self, approval, choice, *, actor_ref, delivery_ref=None):
        mid = approval.message_id if delivery_ref is None else 999_999
        user = int(actor_ref)
        await self.bot.handle_update({"update_id": 1, "callback_query": {
            "id": "q", "from": {"id": user}, "data": f"v2:{approval.approval_id}:{choice}",
            "message": {"message_id": mid, "chat": {"id": approval.chat_id}}}})
        toast = [p["text"] for m, p in self.api.calls if m == "answerCallbackQuery"][-1]
        return next(k for k, pre in _TOAST_PREFIX.items() if toast.startswith(pre))

    def sent_count(self):
        return sum(1 for m, p in self.api.calls if m == "sendMessage")

    def fail_next_send(self, *, permanent, reason):
        code = {"blocked": 403, "unauthorized": 401}.get(reason, 500) if permanent else 500
        self.api.once["sendMessage"] = lambda: TelegramError("sendMessage", code, f"{reason} {self.secret}")

    def break_updates(self):
        self.api.always["editMessageText"] = lambda: TelegramError("editMessageText", 500, "boom")


@pytest.mark.parametrize("harness_cls", [FakeHarness, TelegramHarness], ids=["fake", "telegram"])
@pytest.mark.parametrize("check", CONTRACT_CHECKS, ids=[c.__name__ for c in CONTRACT_CHECKS])
def test_channel_contract(tmp_path, harness_cls, check):
    h = harness_cls(tmp_path)
    try:
        asyncio.run(check(h))
    finally:
        h.store.close()
