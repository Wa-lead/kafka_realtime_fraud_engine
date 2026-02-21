import sys, os, json, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'kafka'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'fraud'))

from consumer import Consumer
from fraud_engine import FraudEngine

FEATURE_CONFIGS = [
    {"name": "sum_txn_1h",       "type": "sum",    "field": "amount",      "window": 3600,  "bucket_size": 600,  "source": "transaction"},
    {"name": "count_txn_1h",     "type": "count",  "field": None,          "window": 3600,  "bucket_size": 600,  "source": "transaction"},
    {"name": "sum_txn_24h",      "type": "sum",    "field": "amount",      "window": 86400, "bucket_size": 3600, "source": "transaction"},
    {"name": "count_txn_24h",    "type": "count",  "field": None,          "window": 86400, "bucket_size": 3600, "source": "transaction"},
    {"name": "unique_ben_24h",   "type": "unique", "field": "beneficiary", "window": 86400, "bucket_size": 3600, "source": "transaction"},
    {"name": "count_credit_24h", "type": "count",  "field": None,          "window": 86400, "bucket_size": 3600, "source": "transaction",
     "filter": {"txn_type": "credit"}},
    {"name": "count_cashout_1h", "type": "count",  "field": None,          "window": 3600,  "bucket_size": 600,  "source": "transaction",
     "filter": {"txn_type": "cashout"}},

    # Account features (source: "account-opening")
    {"name": "account_age_days", "type": "latest", "field": "account_age_days", "source": "account-opening", "default": 9999},
    {"name": "account_type",     "type": "latest", "field": "account_type",     "source": "account-opening", "default": "unknown"},
    {"name": "nationality",      "type": "latest", "field": "nationality",      "source": "account-opening", "default": "unknown"},

    # Card features (source: "card-issue")
    {"name": "has_credit_card",  "type": "latest", "field": "has_credit_card",  "source": "card-issue", "default": 0},
    {"name": "card_type",        "type": "latest", "field": "card_type",        "source": "card-issue", "default": "none"},
    {"name": "credit_limit",     "type": "latest", "field": "credit_limit",     "source": "card-issue", "default": 0},
]

# -- rules
RULES = {
    "high_velocity_high_amount": [
        {"field": "count_txn_1h", "source": "features", "op": ">=", "value": 3},
        {"field": "sum_txn_1h",   "source": "features", "op": ">",  "value": 20000},
    ],
    "suspicious_first_credit": [
        {"field": "count_credit_24h", "source": "features",    "op": "==", "value": 0},
        {"field": "txn_type",         "source": "transaction", "op": "==", "value": "credit"},
        {"field": "amount",           "source": "transaction", "op": ">",  "value": 10000},
    ],
    "many_beneficiaries": [
        {"field": "unique_ben_24h", "source": "features", "op": ">=", "value": 5},
    ],
    "rapid_cashout": [
        {"field": "count_cashout_1h", "source": "features",    "op": ">=", "value": 2},
        {"field": "txn_type",         "source": "transaction", "op": "==", "value": "cashout"},
        {"field": "amount",           "source": "transaction", "op": ">",  "value": 5000},
    ],
    "single_large_txn": [
        {"field": "amount", "source": "transaction", "op": ">", "value": 30000},
    ],
    "new_account_large_txn": [
        {"field": "account_age_days", "source": "features",    "op": "<",  "value": 30},
        {"field": "amount",           "source": "transaction", "op": ">",  "value": 10000},
    ],
    "no_card_large_cashout": [
        {"field": "has_credit_card", "source": "features",    "op": "==", "value": 0},
        {"field": "txn_type",        "source": "transaction", "op": "==", "value": "cashout"},
        {"field": "amount",          "source": "transaction", "op": ">",  "value": 8000},
    ],
}


engine = FraudEngine(FEATURE_CONFIGS, RULES)
stats = {}
enrichment_stats = {"accounts": 0, "cards": 0}



def consume_account_openings():
    consumer = Consumer(client_id='account-enrichment')
    consumer.join_group('account-enrichment', 'account-opening')
    print(f"[account-enrichment] partition {consumer.assigned_partition}")

    while True:
        records = consumer.fetch('account-opening', max_records=50)
        for offset, key, value in records:
            event = json.loads(value)
            event["_source"] = "account-opening"  # tag so feature store routes correctly
            engine.update(event)
            enrichment_stats["accounts"] += 1
        if not records:
            time.sleep(0.3)



def consume_card_issues():
    consumer = Consumer(client_id='card-enrichment')
    consumer.join_group('card-enrichment', 'card-issue')
    print(f"[card-enrichment] partition {consumer.assigned_partition}")

    while True:
        records = consumer.fetch('card-issue', max_records=50)
        for offset, key, value in records:
            event = json.loads(value)
            event["_source"] = "card-issue"
            engine.update(event)
            enrichment_stats["cards"] += 1
        if not records:
            time.sleep(0.3)



def consume_transactions(consumer_id):
    consumer = Consumer(client_id=consumer_id)
    consumer.join_group('fraud-engine', 'transactions')
    partition = consumer.assigned_partition
    print(f"[{consumer_id}] partition {partition}")

    stats[consumer_id] = {
        "partition": partition,
        "processed": 0,
        "blocked": 0,
        "approved": 0,
        "rules_fired": {},
    }

    while True:
        try:
            records = consumer.fetch('transactions', max_records=50)
            for offset, key, value in records:
                txn = json.loads(value)
                decision, fired_rules, features = engine.process(txn)

                s = stats[consumer_id]
                s["processed"] += 1
                if decision == "BLOCK":
                    s["blocked"] += 1
                    for rule in fired_rules:
                        s["rules_fired"][rule] = s["rules_fired"].get(rule, 0) + 1
                else:
                    s["approved"] += 1

            if not records:
                time.sleep(0.2)
        except ConnectionError:
            print(f"[{consumer_id}] Lost connection")
            break


def print_stats():
    total_p, total_b, total_a = 0, 0, 0
    all_rules = {}

    print()
    print(f"  Enriched: {enrichment_stats['accounts']} accounts, {enrichment_stats['cards']} cards")
    print()
    print(f"  {'Consumer':<22} {'Part':>4} {'Processed':>10} {'Blocked':>8} {'Approved':>9}")
    print(f"  {'─'*22} {'─'*4} {'─'*10} {'─'*8} {'─'*9}")

    for cid, s in sorted(stats.items()):
        print(f"  {cid:<22} {s['partition']:>4} {s['processed']:>10} {s['blocked']:>8} {s['approved']:>9}")
        total_p += s["processed"]
        total_b += s["blocked"]
        total_a += s["approved"]
        for rule, count in s["rules_fired"].items():
            all_rules[rule] = all_rules.get(rule, 0) + count

    print(f"  {'─'*22} {'─'*4} {'─'*10} {'─'*8} {'─'*9}")
    pct = (total_b / total_p * 100) if total_p > 0 else 0
    print(f"  {'TOTAL':<22} {'':>4} {total_p:>10} {total_b:>8} {total_a:>9}  ({pct:.1f}% blocked)")

    if all_rules:
        print(f"\n  Rules fired:")
        for rule, count in sorted(all_rules.items(), key=lambda x: -x[1]):
            print(f"    {rule:<35} {count:>5}")



if __name__ == '__main__':
    print("=" * 60)
    print("STARTING CONSUMERS")
    print("=" * 60)

    threading.Thread(target=consume_account_openings, daemon=True).start()
    threading.Thread(target=consume_card_issues, daemon=True).start()

    for i in range(4):
        threading.Thread(target=consume_transactions, args=(f'fraud-consumer-{i}',), daemon=True).start()

    print()
    print("6 consumers running. Ctrl+C to stop.")
    print("=" * 60)

    try:
        while True:
            time.sleep(5)
            print_stats()
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("FINAL")
        print("=" * 60)
        print_stats()