"""领域资料的离线校验。"""
from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("validate_contract", ROOT / "tools" / "validate_contract.py")
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ContractTest(unittest.TestCase):
    def test_contract_examples_and_policies_are_consistent(self) -> None:
        entity_count, event_count, policy_count = MODULE.validate()
        self.assertGreaterEqual(entity_count, 5)
        self.assertGreaterEqual(event_count, 4)
        self.assertGreaterEqual(policy_count, 3)


if __name__ == "__main__":
    unittest.main()
