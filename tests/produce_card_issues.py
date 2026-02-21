import sys, os, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'kafka'))

import numpy as np
from producer import Producer

rng = np.random.default_rng(55)

customers = [f"cust_{i:04d}" for i in range(100)]

producer = Producer(client_id='card-producer')
sent = 0

print("Producing card issues at ~0.5/sec")
print()

try:
    while True:
        customer = rng.choice(customers)
        card_type = str(rng.choice(["debit", "credit", "prepaid"], p=[0.5, 0.35, 0.15]))

        event = {
            "customer_id": customer,
            "card_type": card_type,
            "card_tier": str(rng.choice(["standard", "gold", "platinum"], p=[0.6, 0.3, 0.1])),
            "credit_limit": int(rng.choice([5000, 10000, 25000, 50000, 100000])) if card_type == "credit" else 0,
            "has_credit_card": 1 if card_type == "credit" else 0,
            "timestamp": int(time.time()),
        }

        producer.send('card-issue', customer, json.dumps(event))
        sent += 1

        if sent % 10 == 0:
            print(f"  sent {sent} card events")

        time.sleep(rng.exponential(1.0 / 0.5))

except KeyboardInterrupt:
    print(f"\nStopped. Sent {sent} card events.")
    producer.close()
