import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("load", Path(__file__).with_name("load.py"))
load = importlib.util.module_from_spec(spec)
spec.loader.exec_module(load)


class LoadAccounting(unittest.TestCase):
    def test_nearest_rank_and_empty_latency(self):
        self.assertIsNone(load.percentile([], 99))
        self.assertEqual(load.percentile(list(range(1, 101)), 99), 99)
        self.assertEqual(load.percentile([5], 95), 5)

    def test_bulk_partial_or_empty_response_is_not_success(self):
        self.assertFalse(load.confirmed("bulk", 2, 200, {"errors": True, "items": [{"index": {"status": 201}}]}))
        self.assertFalse(load.confirmed("bulk", 1, 200, {"errors": False, "items": []}))
        self.assertFalse(load.confirmed("bulk", 1, 200, {"errors": False, "items": [{"index": None}]}))
        self.assertTrue(load.confirmed("bulk", 1, 200, {"errors": False, "items": [{"index": {"status": 201}}]}))
        self.assertFalse(load.confirmed("native", 2, 200, {"committed": 1}))


if __name__ == "__main__":
    unittest.main()
