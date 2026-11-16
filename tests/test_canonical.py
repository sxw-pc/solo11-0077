import unittest

from shardroute.canonical import (
    composite_key,
    fingerprint,
    normalize_components,
    normalize_value,
    sha256_text,
    stable_id,
)
from shardroute.errors import ShardRouteError

class CanonicalTests(unittest.TestCase):
    def test_normalize_trims_and_folds_case(self):
        self.assertEqual(normalize_value("  Region_Alpha  "), "Region_Alpha")
        self.assertEqual(normalize_value("  Region_Alpha  ", False), "region_alpha")
        self.assertIsNone(normalize_value(None))
        self.assertEqual(normalize_value(17), "17")
    def test_composite_key_uses_unit_separator(self):
        parts = normalize_components([" tenant_demo_001 ", "region alpha"], False)
        self.assertEqual(parts, ("tenant_demo_001", "region alpha"))
        self.assertEqual(composite_key(parts), "tenant_demo_001\x1fregion alpha")
    def test_container_value_is_rejected(self):
        with self.assertRaises(ShardRouteError) as caught:
            normalize_value(["not", "scalar"])
        self.assertEqual(caught.exception.code, "ROUTE_BAD_VALUE")
    def test_fingerprint_ignores_object_key_order(self):
        left = {"b": 2, "a": {"y": 1, "x": 0}}
        right = {"a": {"x": 0, "y": 1}, "b": 2}
        self.assertEqual(fingerprint(left), fingerprint(right))
        self.assertEqual(len(fingerprint(left)), 64)
    def test_hash_and_stable_id_are_stable(self):
        self.assertEqual(sha256_text("demo"), sha256_text("demo"))
        self.assertEqual(stable_id("fnd", "A", {"b": 2}), stable_id("fnd", "A", {"b": 2}))
        self.assertNotEqual(stable_id("fnd", "A"), stable_id("fnd", "B"))
