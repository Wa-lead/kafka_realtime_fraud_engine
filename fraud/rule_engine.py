class RuleEngine:
    def __init__(self, rules):
        self.rules = rules

    def _check_condition(self, actual, op, expected):
        if op == "==":
            return actual == expected
        elif op == "!=":
            return actual != expected
        elif op == ">":
            return actual > expected
        elif op == ">=":
            return actual >= expected
        elif op == "<":
            return actual < expected
        elif op == "<=":
            return actual <= expected
        return False

    def evaluate(self, transaction, features):
        """Evaluate all rules. Returns list of rule names that fired."""
        fired = []

        for rule_name, conditions in self.rules.items():
            all_true = True

            for condition in conditions:
                # Get the actual value from the right source
                if condition["source"] == "features":
                    actual = features.get(condition["field"], 0)
                else:
                    actual = transaction.get(condition["field"])

                # Check the condition
                if not self._check_condition(actual, condition["op"], condition["value"]):
                    all_true = False
                    break

            if all_true:
                fired.append(rule_name)

        return fired
