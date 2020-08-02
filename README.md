# Kafka challenge by Doodle

### Prerequisites

* you should have Kafka 2.4 installed and running
* 2 Kafka topics **user-event** and **user-stat-event** with 1 partition each should be created manually. Something like:

```bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic user-stat-event```

```bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic user-event```
* recommend using Python 3.8 with pip

### Installation


Install required Python3 library dependencies via the following command.

```
pip install -U faust
```

This will install the following:
* Faust (stream processing library)

### CODING STANDARDS:
Code conforms to [PEP-8](https://www.python.org/dev/peps/pep-0008/) standards for Python coding if/when possible.

PEP information can be found at:  https://www.python.org/dev/peps/

### KEY COMPONENTS / FILES:
* README.md                                 - This file
* kafka_app.py                              - Main app
* kafka_consumer_loop.py                    - The old basic consumer loop script, can be ignored

### HOW TO RUN:

1. Follow Prerequisites and Installation sections above
2. Run the app:
```
python3 kafka_app.py worker
```
3. For testing's sake the app produces 10 events/sec. If you want to simulate events producing by any other method, please comment lines 71-73. Feel free to test on stream.jsonl.gz with any input method.
