from feature_store import FeatureStore
from rule_engine import RuleEngine


class FraudEngine:
    def __init__(self, feature_configs, rules):
        self.feature_store = FeatureStore(feature_configs)
        self.rule_engine = RuleEngine(rules)

    def process(self, transaction):
        cid = transaction["customer_id"]
        ts = transaction["timestamp"]

        # 1. Read ALL features — bucketed and static, no distinction
        features = self.feature_store.read_features(cid, ts)

        # 2. Evaluate rules
        fired_rules = self.rule_engine.evaluate(transaction, features)

        # 3. Decision
        decision = "BLOCK" if fired_rules else "APPROVE"

        # 4. Update features for next transaction
        self.feature_store.update(transaction)

        return decision, fired_rules, features

    def update(self, event):
        """Update feature store from any event source."""
        self.feature_store.update(event)