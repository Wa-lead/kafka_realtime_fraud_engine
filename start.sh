#!/bin/bash
trap "kill 0; exit" SIGINT

echo "Starting broker..."
python tests/start_broker.py &
sleep 1

echo "Starting producers..."
python tests/produce_transactions.py &
python tests/produce_account_openings.py &
python tests/produce_card_issues.py &
sleep 1

echo "Starting consumers..."
python tests/start_consumers.py &

wait