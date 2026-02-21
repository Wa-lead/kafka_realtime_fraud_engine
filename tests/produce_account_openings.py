import sys, os, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'kafka'))

import numpy as np
from producer import Producer

rng = np.random.default_rng(77)

customers = [f"cust_{i:04d}" for i in range(100)]

producer = Producer(client_id='account-producer')
sent = 0

print("Producing account openings at ~1/sec")
print()

try:
    while True:
        customer = rng.choice(customers)

        event = {
            "customer_id": customer,
            "account_type": str(rng.choice(["savings", "checking", "business"], p=[0.5, 0.35, 0.15])),
            "initial_deposit": int(rng.uniform(500, 100000)),
            "account_age_days": int(rng.exponential(365)),
            "nationality": str(rng.choice(["SA", "SA", "SA", "AE", "EG", "JO", "PK", "IN"])),
            "timestamp": int(time.time()),
        }

        producer.send('account-opening', customer, json.dumps(event))
        sent += 1

        if sent % 10 == 0:
            print(f"  sent {sent} account events")

        time.sleep(rng.exponential(1.0 / 1.0))

except KeyboardInterrupt:
    print(f"\nStopped. Sent {sent} account events.")
    producer.close()
