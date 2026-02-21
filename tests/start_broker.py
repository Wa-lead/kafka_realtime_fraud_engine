import sys, os, shutil
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'kafka'))

from broker import Broker

DATA_DIR = './broker_data'

if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)

broker = Broker(log_dir=DATA_DIR)

broker.create_topic('transactions', num_partitions=4)
broker.create_topic('account-opening', num_partitions=2)
broker.create_topic('card-issue', num_partitions=2)

print()

try:
    broker.start()
except KeyboardInterrupt:
    print("\nBroker stopped.")
