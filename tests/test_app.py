import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from pathlib import Path

from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo, PeerChannel

from telegram_worker.app import (
    BackfillRequest,
    DeliveryLedger,
    ForwardRequest,
    MessageUnit,
    ResourceBlock,
    StateStore,
    TelegramForwarder,
    ReconcileTargetRequest,
    ResolveSourceRequest,
    load_dotenv,
    media_filename,
    make_resources,
    readable_duration,
    readable_message_text,
    readable_message_text_with_album,
    resolve_message_caption,
    resource_rate,
    video_duration,
)


class WorkerTest(unittest.TestCase):
    def test_request_validation_and_video_duration(self):
        request = ForwardRequest.parse(
            {
                "source": "@source",
                "target": "@target",
                "limit": 10,
                "minVideoDuration": 300,
                "markRead": True,
            }
        )
        self.assertEqual(request.limit, 10)
        self.assertTrue(request.mark_read)
        video = SimpleNamespace(
            document=SimpleNamespace(
                attributes=[DocumentAttributeFilename("a.mp4"), DocumentAttributeVideo(301, 1, 1)]
            )
        )
        self.assertEqual(video_duration(video), 301)
        self.assertEqual(
            media_filename(
                SimpleNamespace(
                    document=SimpleNamespace(
                        attributes=[DocumentAttributeFilename("example.mp4")]
                    )
                )
            ),
            "example.mp4",
        )
        self.assertIsNone(video_duration(SimpleNamespace(document=None)))
        self.assertEqual(readable_duration(6966.133), "1小时56分6秒")
        self.assertEqual(
            readable_message_text(SimpleNamespace(raw_text="第一行\n  第二行")),
            "第一行 第二行",
        )
        self.assertFalse(ForwardRequest.parse({"source": "s", "target": "t"}).mark_read)
        self.assertTrue(
            ForwardRequest.parse({"source": "s", "target": "t", "dryRun": True}).dry_run
        )
        self.assertEqual(
            ForwardRequest.parse({"source": -1001735089356, "target": "t"}).source,
            -1001735089356,
        )
        self.assertEqual(
            ResolveSourceRequest.parse(
                {"source": "https://t.me/c/3789958298/602"}
            ).source,
            -1003789958298,
        )
        backfill = BackfillRequest.parse({"source": "s", "target": "t"})
        self.assertEqual(backfill.lookback_days, 180)
        self.assertEqual(backfill.top_resources, 10)
        self.assertEqual(backfill.max_messages, 5000)
        self.assertEqual(
            BackfillRequest.parse(
                {"source": "s", "target": "t", "startMode": "latest"}
            ).start_mode,
            "latest",
        )
        with self.assertRaises(ValueError):
            ForwardRequest.parse({"source": "s", "target": "t", "markRead": "false"})
        with self.assertRaises(ValueError):
            ForwardRequest.parse({"source": True, "target": "t"})
        with self.assertRaises(ValueError):
            ForwardRequest.parse({"source": "s", "target": "t", "dryRun": "true"})

    def test_state_store_persists_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore(Path(directory) / "state.json")
            self.assertIsNone(store.cursor(-100123))
            store.save_cursor(-100123, 456)
            self.assertEqual(store.cursor(-100123), 456)
            store.save_completed_group(-100123, 90, "101,102")
            self.assertEqual(store.completed_groups(-100123, 90), {"101,102"})
            store.clear_progress(-100123, 90)
            self.assertEqual(store.completed_groups(-100123, 90), set())
            store.save_backfill_before(-100123, 400)
            self.assertEqual(store.backfill_before(-100123), 400)

    def test_delivery_ledger_persists_cross_mode_deduplication(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = DeliveryLedger(Path(directory) / "worker.db")
            ledger.record(
                target_id=456,
                source_id=-100123,
                group_key="101,102",
                resource_key="101",
                source_message_ids=[101, 102],
                target_message_ids=[9001, 9002],
                delivery_mode="FOLLOW",
            )

            delivery = DeliveryLedger(Path(directory) / "worker.db").get(
                456, -100123, "101,102"
            )
            self.assertIsNotNone(delivery)
            self.assertEqual(delivery.target_message_ids, [9001, 9002])
            self.assertEqual(delivery.delivery_mode, "FOLLOW")
            ledger.record_command_result("run-1:source", "FORWARD", {"sent": 2})
            self.assertEqual(
                ledger.command_result("run-1:source", "FORWARD"), {"sent": 2}
            )

    def test_hashtag_resource_uses_anchor_rate(self):
        anchor = SimpleNamespace(
            id=90,
            grouped_id=None,
            raw_text="#作者",
            document=SimpleNamespace(attributes=[DocumentAttributeVideo(60, 1, 1)]),
            views=1000,
            forwards=10,
        )
        body = SimpleNamespace(
            id=101,
            grouped_id=None,
            raw_text="",
            document=SimpleNamespace(attributes=[DocumentAttributeVideo(600, 1, 1)]),
            views=10000,
            forwards=500,
        )
        resource = ResourceBlock([MessageUnit([body])], MessageUnit([anchor], anchor.raw_text))
        request = ForwardRequest(
            "source", "target", None, 300,
            min_views=500,
            min_forwards=1,
            resource_mode="hashtag_resource",
        )
        self.assertEqual(resource_rate(resource, request), 0.01)
        self.assertEqual(
            ResourceBlock([MessageUnit([body], "普通资源说明")]).caption,
            "普通资源说明",
        )

    def test_load_dotenv_does_not_override_process_environment(self):
        import os

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("WORKER_TEST_VALUE=file\n", encoding="utf-8")
            os.environ["WORKER_TEST_VALUE"] = "process"
            load_dotenv(path)
            self.assertEqual(os.environ["WORKER_TEST_VALUE"], "process")
            del os.environ["WORKER_TEST_VALUE"]


class ForwarderTest(unittest.IsolatedAsyncioTestCase):
    async def test_backfill_latest_expands_top_n_without_resending_previous_top(self):
        now = datetime.now(timezone.utc)

        class BackfillClient:
            def __init__(self):
                self.forwarded = []
                self.messages = [
                    SimpleNamespace(
                        id=message_id,
                        grouped_id=None,
                        raw_text=f"资源 {message_id}",
                        media=SimpleNamespace(),
                        document=SimpleNamespace(
                            attributes=[DocumentAttributeVideo(600, 1, 1)]
                        ),
                        date=now - timedelta(days=1),
                        views=10000,
                        forwards=message_id * 100,
                    )
                    for message_id in range(1, 4)
                ]

            async def get_entity(self, value):
                return PeerChannel(123 if value == "source" else 456)

            async def iter_messages(self, _source, **kwargs):
                messages = sorted(self.messages, key=lambda message: message.id, reverse=True)
                if "max_id" in kwargs:
                    messages = [message for message in messages if message.id < kwargs["max_id"]]
                for message in messages[:kwargs.get("limit")]:
                    yield message

            async def forward_messages(self, _target, message_ids, _from_peer=None, **kwargs):
                self.forwarded.append(message_ids)
                return [SimpleNamespace(id=9000 + len(self.forwarded))]

        with tempfile.TemporaryDirectory() as directory:
            state = StateStore(Path(directory) / "state.json")
            client = BackfillClient()
            forwarder = TelegramForwarder(client, state)
            first = await forwarder.backfill(
                BackfillRequest(
                    "source", "target", top_resources=1,
                    group_interval_seconds=0, resource_interval_seconds=0,
                    request_id="first",
                )
            )
            expanded = await forwarder.backfill(
                BackfillRequest(
                    "source", "target", top_resources=2, start_mode="latest",
                    group_interval_seconds=0, resource_interval_seconds=0,
                    request_id="expanded",
                )
            )
            replayed = await forwarder.backfill(
                BackfillRequest(
                    "source", "target", top_resources=2, start_mode="latest",
                    group_interval_seconds=0, resource_interval_seconds=0,
                    request_id="expanded",
                )
            )

            self.assertEqual(first["forwardedResourceCount"], 1)
            self.assertEqual(expanded["selectedResourceCount"], 2)
            self.assertEqual(expanded["duplicateResourceCount"], 1)
            self.assertEqual(expanded["forwardedResourceCount"], 1)
            self.assertEqual(client.forwarded, [[3], [2]])
            self.assertTrue(replayed["replayedResponse"])
            self.assertEqual(client.forwarded, [[3], [2]])

    async def test_reconcile_target_records_native_manual_forwards(self):
        class ReconcileClient:
            async def get_entity(self, _value):
                return PeerChannel(456)

            async def iter_messages(self, _target, **_kwargs):
                for target_id, source_id in ((10, 101), (11, 102)):
                    yield SimpleNamespace(
                        id=target_id,
                        grouped_id=999,
                        fwd_from=SimpleNamespace(
                            from_id=PeerChannel(123),
                            channel_post=source_id,
                        ),
                    )

        with tempfile.TemporaryDirectory() as directory:
            state = StateStore(Path(directory) / "state.json")
            forwarder = TelegramForwarder(ReconcileClient(), state)
            result = await forwarder.reconcile_target(
                ReconcileTargetRequest("target")
            )
            delivery = forwarder.ledger.get(
                -1000000000456, -1000000000123, "101,102"
            )

            self.assertEqual(result["reconciledGroupCount"], 1)
            self.assertIsNotNone(delivery)
            self.assertEqual(delivery.delivery_mode, "MANUAL_RECONCILED")

    async def test_backfill_dry_run_ranks_without_moving_history_cursor(self):
        now = datetime.now(timezone.utc)

        class BackfillClient:
            def __init__(self):
                self.entities = []
                self.messages = [
                    SimpleNamespace(
                        id=message_id,
                        grouped_id=None,
                        raw_text="",
                        document=SimpleNamespace(
                            attributes=[DocumentAttributeVideo(600, 1, 1)]
                        ),
                        date=now - timedelta(days=message_id),
                        views=10000,
                        forwards=message_id * 100,
                    )
                    for message_id in range(1, 4)
                ]

            async def get_entity(self, value):
                self.entities.append(value)
                return PeerChannel(123)

            async def iter_messages(self, _source, **kwargs):
                messages = sorted(self.messages, key=lambda message: message.id, reverse=True)
                if "max_id" in kwargs:
                    messages = [message for message in messages if message.id < kwargs["max_id"]]
                for message in messages[:kwargs.get("limit")]:
                    yield message

        with tempfile.TemporaryDirectory() as directory:
            state = StateStore(Path(directory) / "state.json")
            client = BackfillClient()
            result = await TelegramForwarder(client, state).backfill(
                BackfillRequest("source", "target", top_resources=2, dry_run=True)
            )

            self.assertEqual(client.entities, ["source", "target"])
            self.assertEqual(result["selectedResourceCount"], 2)
            self.assertEqual(
                [item["sourceMessageIds"] for item in result["selectedResources"]],
                [[3], [2]],
            )
            self.assertIsNone(state.backfill_before(-1000000000123))

    async def test_resolve_source_refreshes_dialogs_for_uncached_private_channel(self):
        class ResolveClient:
            def __init__(self):
                self.resolve_attempts = 0
                self.refreshed = False

            async def get_entity(self, value):
                self.asserted_value = value
                self.resolve_attempts += 1
                if self.resolve_attempts == 1:
                    raise ValueError("entity not cached")
                return PeerChannel(3789958298)

            async def get_dialogs(self):
                self.refreshed = True

        with tempfile.TemporaryDirectory() as directory:
            client = ResolveClient()
            result = await TelegramForwarder(
                client, StateStore(Path(directory) / "state.json")
            ).resolve_source(
                ResolveSourceRequest.parse(
                    {"source": "https://t.me/c/3789958298/602"}
                )
            )

            self.assertEqual(client.asserted_value, -1003789958298)
            self.assertEqual(client.resolve_attempts, 2)
            self.assertTrue(client.refreshed)
            self.assertEqual(result["sourceId"], -1003789958298)

    async def test_empty_backfill_keeps_history_response_shape(self):
        class EmptyBackfillClient:
            async def get_entity(self, value):
                return PeerChannel(123 if value == "source" else 456)

            async def iter_messages(self, _source, **_kwargs):
                if False:
                    yield None

        with tempfile.TemporaryDirectory() as directory:
            state = StateStore(Path(directory) / "state.json")
            state.save_backfill_before(-1000000000123, 1)
            result = await TelegramForwarder(
                EmptyBackfillClient(), state
            ).backfill(
                BackfillRequest("source", "target", dry_run=True)
            )

            self.assertEqual(result["scanned"], 0)
            self.assertEqual(result["selectedResources"], [])
            self.assertEqual(result["forwardResults"], [])
            self.assertEqual(result["forwardedResourceCount"], 0)
            self.assertEqual(result["appliedRules"]["selection"], "TOP_N")

    async def test_hashtag_groups_are_copied_with_same_caption_and_resume(self):
        anchor = MessageUnit(
            [SimpleNamespace(id=90)],
            "统一说明 #作者",
        )
        units = [
            MessageUnit([SimpleNamespace(id=101, media="media-101")]),
            MessageUnit([SimpleNamespace(id=102, media="media-102")]),
        ]
        resource = ResourceBlock(units, anchor)

        class CopyClient:
            def __init__(self):
                self.calls = []

            async def send_file(self, target, media, **kwargs):
                self.calls.append((target, media, kwargs))
                return [SimpleNamespace(id=9000 + len(self.calls))]

        with tempfile.TemporaryDirectory() as directory:
            client = CopyClient()
            state = StateStore(Path(directory) / "state.json")
            forwarder = TelegramForwarder(client, state)
            request = ForwardRequest(
                "source", "target", None, 300,
                resource_mode="hashtag_resource",
                group_interval_seconds=0,
            )
            first = await forwarder._send_resource_groups(
                PeerChannel(456), "source", -100123, resource, request
            )
            second = await forwarder._send_resource_groups(
                PeerChannel(456), "source", -100123, resource, request
            )

            self.assertEqual(first["targetMessageIds"], [9001, 9002])
            self.assertEqual(first["forwardedGroupCount"], 2)
            self.assertEqual(second["forwardedGroupCount"], 0)
            self.assertEqual(second["duplicateGroupCount"], 2)
            self.assertEqual(len(client.calls), 2)
            self.assertEqual(client.calls[0][2]["caption"], ["统一说明 #作者"])
            self.assertEqual(client.calls[1][2]["caption"], ["统一说明 #作者"])
            self.assertEqual(
                state.completed_groups(-100123, 90),
                {"101", "102"},
            )

    async def test_follow_and_backfill_share_the_same_delivery_ledger(self):
        resource = ResourceBlock(
            [MessageUnit([SimpleNamespace(id=101, media="media-101")])]
        )

        class ForwardClient:
            def __init__(self):
                self.calls = []

            async def forward_messages(self, target, message_ids, **_kwargs):
                self.calls.append((target, message_ids))
                return [SimpleNamespace(id=9000 + len(self.calls))]

        with tempfile.TemporaryDirectory() as directory:
            client = ForwardClient()
            forwarder = TelegramForwarder(
                client, StateStore(Path(directory) / "state.json")
            )
            request = ForwardRequest(
                "source", "target", None, 300,
                group_interval_seconds=0,
            )

            followed = await forwarder._send_resource_groups(
                PeerChannel(456), PeerChannel(123), -100123,
                resource, request, "FOLLOW",
            )
            backfilled = await forwarder._send_resource_groups(
                PeerChannel(456), PeerChannel(123), -100123,
                resource, request, "BACKFILL",
            )

            self.assertEqual(followed["forwardedGroupCount"], 1)
            self.assertEqual(backfilled["forwardedGroupCount"], 0)
            self.assertEqual(backfilled["duplicateGroupCount"], 1)
            self.assertEqual(len(client.calls), 1)

    async def test_hashtag_context_stitches_resource_across_cursor(self):
        old = datetime.now(timezone.utc) - timedelta(days=2)
        anchor = SimpleNamespace(
            id=100,
            grouped_id=None,
            raw_text="资源说明 #作者",
            document=None,
            date=old,
        )
        body = SimpleNamespace(
            id=101,
            grouped_id=None,
            raw_text="",
            document=SimpleNamespace(attributes=[DocumentAttributeVideo(600, 1, 1)]),
            date=old,
        )

        class ContextClient:
            async def iter_messages(self, _source, **kwargs):
                if "max_id" in kwargs:
                    yield anchor

        with tempfile.TemporaryDirectory() as directory:
            forwarder = TelegramForwarder(
                ContextClient(), StateStore(Path(directory) / "state.json")
            )
            messages = await forwarder._with_anchor_context(
                "source", 100, [body], "hashtag_resource"
            )
            resources = make_resources(messages, "hashtag_resource")

        self.assertEqual([message.id for message in messages], [100, 101])
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources[0].caption, "资源说明 #作者")
        self.assertEqual([message.id for message in resources[0].messages], [101])

    async def test_adaptive_baseline_stops_at_target_and_uses_cache(self):
        old = datetime.now(timezone.utc) - timedelta(days=2)

        class BaselineClient:
            def __init__(self):
                self.calls = 0
                self.messages = [
                    SimpleNamespace(
                        id=message_id,
                        grouped_id=None,
                        raw_text="",
                        document=(
                            SimpleNamespace(attributes=[DocumentAttributeVideo(301, 1, 1)])
                            if message_id % 4 == 0
                            else None
                        ),
                        date=old,
                        views=10000,
                        forwards=100,
                    )
                    for message_id in range(1, 601)
                ]

            async def iter_messages(self, _source, *, max_id, limit):
                self.calls += 1
                eligible = sorted(
                    (message for message in self.messages if message.id < max_id),
                    key=lambda message: message.id,
                    reverse=True,
                )
                for message in eligible[:limit]:
                    yield message

        with tempfile.TemporaryDirectory() as directory:
            client = BaselineClient()
            forwarder = TelegramForwarder(
                client, StateStore(Path(directory) / "state.json")
            )
            request = ForwardRequest("source", "target", None, 300)
            now = datetime.now(timezone.utc)
            result = await forwarder._estimate_baseline(
                "source", -100123, 1000, request, now
            )
            cached = await forwarder._estimate_baseline(
                "source", -100123, 1200, request, now
            )

        self.assertEqual(result.sample_count, 100)
        self.assertEqual(result.scanned_messages, 400)
        self.assertEqual(result.confidence, "ideal")
        self.assertEqual(client.calls, 2)
        self.assertTrue(cached.cache_hit)

    async def test_reads_caption_from_another_message_in_the_same_album(self):
        grouped_id = 123456
        video = SimpleNamespace(id=20, grouped_id=grouped_id, raw_text="")
        caption = SimpleNamespace(id=17, grouped_id=grouped_id, raw_text="相册说明")

        class AlbumClient:
            async def get_messages(self, _source, *, ids):
                self.requested_ids = ids
                return [caption, video]

        client = AlbumClient()
        result = await readable_message_text_with_album(client, "source", video, {})
        caption_text, caption_message_id = await resolve_message_caption(
            client, "source", video, {}
        )

        self.assertEqual(result, "相册说明（来自相册消息 17）")
        self.assertEqual(caption_text, "相册说明")
        self.assertEqual(caption_message_id, 17)
        self.assertIn(17, client.requested_ids)

    async def test_starts_at_unread_cursor_and_forwards_matching_video(self):
        old = datetime.now(timezone.utc) - timedelta(days=2)
        class FakeClient:
            def __init__(self):
                self.forwarded = []
                self.read_through = None
                self.album_caption = SimpleNamespace(
                    id=99,
                    grouped_id=888,
                    raw_text="相册说明",
                )
                self.messages = [
                    SimpleNamespace(id=101, grouped_id=None, raw_text="", document=None, date=old),
                    SimpleNamespace(
                        id=102,
                        grouped_id=888,
                        raw_text="",
                        media=SimpleNamespace(name="video-102"),
                        document=SimpleNamespace(
                            attributes=[DocumentAttributeVideo(301, 1, 1)]
                        ),
                        date=old,
                        views=10000,
                        forwards=100,
                    ),
                    SimpleNamespace(
                        id=103,
                        grouped_id=None,
                        raw_text="",
                        media=SimpleNamespace(name="video-103"),
                        document=SimpleNamespace(
                            attributes=[DocumentAttributeVideo(300, 1, 1)]
                        ),
                        date=old,
                        views=10000,
                        forwards=100,
                    ),
                ]
                self.history = [
                    SimpleNamespace(
                        id=message_id,
                        grouped_id=None,
                        raw_text="",
                        document=SimpleNamespace(
                            attributes=[DocumentAttributeVideo(301, 1, 1)]
                        ),
                        date=old,
                        views=10000,
                        forwards=100,
                    )
                    for message_id in range(1, 51)
                ]

            async def get_entity(self, value):
                return PeerChannel(123 if value == "source" else 456)

            async def get_input_entity(self, value):
                return value

            async def __call__(self, _request):
                return SimpleNamespace(dialogs=[SimpleNamespace(read_inbox_max_id=100)])

            async def iter_messages(self, _source, **_kwargs):
                source = self.history if "max_id" in _kwargs else self.messages
                for message in source:
                    yield message

            async def get_messages(self, _source, *, ids):
                return [self.album_caption, self.messages[1]]

            async def forward_messages(self, target, message_ids, from_peer):
                self.forwarded.append((target.channel_id, message_ids, from_peer.channel_id))
                return [SimpleNamespace(id=9001)]

            async def send_read_acknowledge(self, _source, *, max_id):
                self.read_through = max_id
                return True

        with tempfile.TemporaryDirectory() as directory:
            client = FakeClient()
            forwarder = TelegramForwarder(client, StateStore(Path(directory) / "state.json"))
            result = await forwarder.forward_unread(
                ForwardRequest("source", "target", 10, 300, True)
            )

            default_client = FakeClient()
            await TelegramForwarder(default_client, StateStore(Path(directory) / "default.json")).forward_unread(
                ForwardRequest("source", "target", 20, 300)
            )
            self.assertIsNone(default_client.read_through)
            self.assertEqual(default_client.forwarded, [(456, [102], 123)])

            empty_client = FakeClient()
            empty_client.messages = []
            empty_client.history = []
            with self.assertRaisesRegex(RuntimeError, "历史有效资源不足"):
                await TelegramForwarder(empty_client, StateStore(Path(directory) / "empty.json")).forward_unread(
                    ForwardRequest("source", "target", 20, 300, True)
                )

            failed_client = FakeClient()
            async def fail_forward(*args, **kwargs):
                raise RuntimeError("forward failed")
            failed_client.forward_messages = fail_forward
            failed_state = StateStore(Path(directory) / "failed.json")
            with self.assertRaisesRegex(RuntimeError, "forward failed"):
                await TelegramForwarder(failed_client, failed_state).forward_unread(
                    ForwardRequest("source", "target", 20, 300, True)
                )
            self.assertIsNone(failed_client.read_through)
            self.assertEqual(failed_state.cursor(-1000000000123), 101)

        self.assertTrue(result["initializedFromUnread"])
        self.assertEqual(result["matchedMessageIds"], [102])
        self.assertEqual(client.forwarded, [(456, [102], 123)])
        self.assertEqual(len(result["forwardResults"]), 1)
        self.assertEqual(result["forwardResults"][0]["sourceMessageIds"], [102])
        self.assertEqual(result["forwardResults"][0]["targetMessageIds"], [9001])
        self.assertEqual(result["forwardResults"][0]["forwardRate"], 0.01)
        self.assertEqual(result["forwardResults"][0]["status"], "FORWARDED")
        self.assertEqual(client.read_through, 103)
        self.assertEqual(result["markedReadThrough"], 103)
        self.assertEqual(result["nextCursor"], 103)

    async def test_dry_run_does_not_resolve_target_send_or_move_cursor(self):
        old = datetime.now(timezone.utc) - timedelta(days=2)

        class FakeClient:
            def __init__(self):
                self.entities = []
                self.forwarded = []
                self.message = SimpleNamespace(
                    id=101,
                    grouped_id=None,
                    raw_text="测试",
                    document=SimpleNamespace(
                        attributes=[DocumentAttributeVideo(301, 1, 1)]
                    ),
                    date=old,
                    views=10000,
                    forwards=100,
                )
                self.history = [
                    SimpleNamespace(
                        id=message_id,
                        grouped_id=None,
                        raw_text="",
                        document=SimpleNamespace(
                            attributes=[DocumentAttributeVideo(301, 1, 1)]
                        ),
                        date=old,
                        views=10000,
                        forwards=100,
                    )
                    for message_id in range(1, 51)
                ]

            async def get_entity(self, value):
                self.entities.append(value)
                return PeerChannel(123)

            async def get_input_entity(self, value):
                return value

            async def __call__(self, _request):
                return SimpleNamespace(dialogs=[SimpleNamespace(read_inbox_max_id=100)])

            async def iter_messages(self, _source, **_kwargs):
                if "max_id" in _kwargs:
                    for message in self.history:
                        yield message
                else:
                    yield self.message

            async def forward_messages(self, *args, **kwargs):
                self.forwarded.append((args, kwargs))

        with tempfile.TemporaryDirectory() as directory:
            state = StateStore(Path(directory) / "state.json")
            client = FakeClient()
            result = await TelegramForwarder(client, state).forward_unread(
                ForwardRequest("source", "target", None, 300, dry_run=True)
            )

            self.assertEqual(client.entities, ["source", "target"])
            self.assertEqual(client.forwarded, [])
            self.assertIsNone(state.cursor(-1000000000123))
            self.assertTrue(result["dryRun"])
            self.assertEqual(result["selectedResourceCount"], 1)
            self.assertEqual(result["selectedResources"][0]["sourceMessageIds"], [101])
            self.assertEqual(result["nextCursor"], 100)

            young_client = FakeClient()
            young_client.message.date = datetime.now(timezone.utc)
            young_state = StateStore(Path(directory) / "young.json")
            young_result = await TelegramForwarder(young_client, young_state).forward_unread(
                ForwardRequest("source", "target", None, 300)
            )
            self.assertEqual(young_result["deferredResourceCount"], 1)
            self.assertEqual(young_result["nextCursor"], 100)
            self.assertEqual(young_client.forwarded, [])
            self.assertIsNone(young_state.cursor(-1000000000123))


if __name__ == "__main__":
    unittest.main()
