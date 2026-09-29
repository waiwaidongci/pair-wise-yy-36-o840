import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError
from src.repository import Repository
from src.rules import TRANSITION_ROLES, STATES, classify_online_group, classify_retest
from src.service import Service


class BatchDisposalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def batch(self, ref, batch_type, **extra):
        payload = {
            "batch_ref": ref,
            "outlet": "OUTLET-A",
            "batch_type": batch_type,
            "measured_at": f"2026-01-01T0{extra.pop('hour', 0)}:00:00+00:00",
        }
        payload.update(extra)
        return self.service.ingest_batch(payload, "operator", "operator")

    def test_batches_group_deduplicate_merge_and_keep_single_fluctuation(self):
        first = self.batch(
            "B-1", "online", hour=1, permit_limit=10,
            readings=[{"value": 15}, {"value": 16}],
        )
        item_id = first["item"]["id"]
        self.assertEqual(first["item"]["severity"], "exceedance")
        self.assertEqual(first["item"]["current"]["basis"], "online")
        self.assertEqual(first["item"]["version"], 2)

        second = self.batch(
            "B-2", "online", hour=2,
            readings=[{"value": 9}, {"value": 8}],
        )
        self.assertEqual(second["item"]["id"], item_id)

        third = self.batch(
            "B-3", "online", hour=3,
            readings=[{"value": 11}],
        )
        records = self.service.list_records(item_id, "viewer")
        episodes = [r for r in records if r["episode_key"]]
        self.assertEqual(len(episodes), 2)
        self.assertEqual(
            sorted((r["kind"], r["status"]) for r in episodes),
            [("continuous_anomaly", "closed"), ("single_fluctuation", "closed")],
        )
        self.assertEqual(third["item"]["severity"], "watch")

        duplicate = self.batch(
            "B-3", "online", hour=9,
            readings=[{"value": 100}],
        )
        self.assertTrue(duplicate["duplicate"])
        batches = self.service.list_batches(item_id, "viewer")
        self.assertEqual(len(batches), 3)
        self.assertEqual(self.service.get_item(item_id, "viewer")["version"], 4)

    def test_condition_then_retest_recalculates_conclusion_and_preserves_versions(self):
        initial = self.batch(
            "B-10", "online", hour=1, permit_limit=10,
            readings=[{"value": 15}, {"value": 16}],
        )
        item_id = initial["item"]["id"]
        before = initial["item"]

        abnormal = self.batch("B-11", "condition", hour=2, condition_status="abnormal")
        self.assertEqual(abnormal["item"]["severity"], "major")
        self.assertIn("deadline_hours", abnormal["item"])
        self.assertIn("rectification_deadline", abnormal["item"])
        self.assertGreaterEqual(abnormal["item"]["escalation_level"], 2)

        normal_condition = self.batch("B-11b", "condition", hour=3, condition_status="normal")
        self.assertEqual(normal_condition["item"]["severity"], "exceedance")

        passed = self.batch("B-12", "retest", hour=4, value=8)
        self.assertEqual(passed["item"]["severity"], "normal")
        self.assertEqual(passed["item"]["current"]["basis"], "retest")
        self.assertFalse(any(
            e["status"] == "open"
            for e in passed["item"]["current"]["episodes"]
        ))

        versions = self.service.list_versions(item_id, "viewer")
        severities = [v["severity"] for v in versions]
        self.assertEqual(severities[:2], ["normal", "exceedance"])
        self.assertIn("major", severities)
        self.assertEqual(versions[-1]["severity"], "normal")
        self.assertEqual(versions[0]["quantity"], before["quantity"])
        self.assertEqual(passed["item"]["version"], versions[-1]["version"])

        # A stale actor must re-evaluate against the newest version before closing.
        for target in STATES[1:-1]:
            current = self.service.get_item(item_id, "viewer")
            current = self.service.transition(
                item_id, target, current["version"], "reviewer",
                TRANSITION_ROLES[target][0],
            )
        with self.assertRaises(ConflictError):
            self.service.transition(
                item_id, STATES[-1], passed["item"]["version"], "director", "director"
            )
        latest = self.service.get_item(item_id, "viewer")
        closed = self.service.transition(
            item_id, STATES[-1], latest["version"], "director", "director"
        )
        self.assertEqual(closed["status"], "closed")
        self.assertTrue(self.repo.verify_audit_chain())

    def test_permit_limit_change_recalculates_without_losing_history(self):
        initial = self.batch(
            "B-20", "online", hour=1, permit_limit=10,
            readings=[{"value": 15}, {"value": 16}],
        )
        item_id = initial["item"]["id"]
        self.assertEqual(initial["item"]["severity"], "exceedance")

        relaxed = self.batch("B-21", "permit", hour=2, permit_limit=20)
        self.assertEqual(relaxed["item"]["threshold"], 20)
        self.assertEqual(relaxed["item"]["severity"], "normal")
        self.assertEqual(
            self.service.list_batches(item_id, "viewer")[0]["readings"][0]["raw_judgment"],
            "exceedance",
        )

        histories = self.service.list_versions(item_id, "viewer")
        self.assertEqual([v["threshold"] for v in histories[:2]], [10, 10])
        self.assertEqual(histories[-1]["threshold"], 20)
        self.assertEqual(len({v["version"] for v in histories}), len(histories))

    def test_rule_thresholds(self):
        self.assertEqual(classify_online_group(9, 10, 2), "normal")
        self.assertEqual(classify_online_group(11, 10, 1), "watch")
        self.assertEqual(classify_online_group(15, 10, 2), "exceedance")
        self.assertEqual(classify_retest(10, 10), "normal")
        self.assertEqual(classify_retest(15, 10), "exceedance")


if __name__ == "__main__":
    unittest.main()
