import sys, os, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'kafka'))

import numpy as np
from producer import Producer

rng = np.random.default_rng(42)

customers = [f"cust_{i:04d}" for i in range(100)]
fraudsters = set(rng.choice(customers, size=5, replace=False))
beneficiaries = [f"ben_{i:04d}" for i in range(200)]
txn_types = ["debit", "credit", "cashout", "transfer"]

producer = Producer(client_id='txn-producer')
sent = 0

print(f"Producing transactions at ~10/sec")
print(f"Fraudsters: {sorted(str(f) for f in fraudsters)}")
print()

try:
    while True:
        customer = rng.choice(customers)
        is_fraud = customer in fraudsters

        if is_fraud:
            amount = max(100, int(rng.normal(15000, 8000)))
            beneficiary = rng.choice(beneficiaries)
            txn_type = rng.choice(txn_types, p=[0.1, 0.3, 0.4, 0.2])
        else:
            amount = max(10, int(rng.normal(2000, 1500)))
            beneficiary = rng.choice(beneficiaries[:5])
            txn_type = rng.choice(txn_types, p=[0.6, 0.15, 0.1, 0.15])

        txn = {
            "customer_id": customer,
            "amount": amount,
            "beneficiary": str(beneficiary),
            "txn_type": str(txn_type),
            "timestamp": int(time.time()),
        }

        producer.send('transactions', customer, json.dumps(txn))
        sent += 1

        if sent % 50 == 0:
            print(f"  sent {sent} transactions")

        time.sleep(rng.exponential(1.0 / 10.0))

except KeyboardInterrupt:
    print(f"\nStopped. Sent {sent} transactions.")
    producer.close()
