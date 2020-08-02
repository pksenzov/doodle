import sys
import logging

from confluent_kafka import Consumer
from confluent_kafka.cimpl import KafkaError, KafkaException

MIN_COMMIT_COUNT = 10000
KAFKA = 'localhost:9092'
GROUP = 'my_consumer_group'
TOPICS = ['user-event']
TIMEOUT = 1.0
SESSION_TIMEOUT = 6000
AUTO_OFFSET_RESET = 'earliest'

# Create logger
logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

def commit_completed(err, partitions):
    if err:
        logger.error(str(err))
    else:
        logger.info("Committed partition offsets: " + str(partitions))

def msg_process(msg):
    logger.info('Received message: {}'.format(msg.value().decode('utf-8')))

def consume_loop():
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': KAFKA,
            'group.id': GROUP,
            'session.timeout.ms': SESSION_TIMEOUT,
            'auto.offset.reset': AUTO_OFFSET_RESET,
            'on_commit': commit_completed}

    # Create Consumer instance. Logs will be emitted when poll() is called
    c = Consumer(conf, logger=logger)

    def print_assignment(_, partitions):
        logger.info('Assignment: {}'.format(partitions))

    # Subscribe to topics
    c.subscribe(TOPICS, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        msg_count = 0
        while True:
            msg = c.poll(timeout=TIMEOUT)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                msg_process(msg)
                msg_count += 1
                # Manually commit every MIN_COMMIT_COUNT messages
                if msg_count % MIN_COMMIT_COUNT == 0:
                    c.commit()

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()

if __name__ == '__main__':
    # Basic consume loop without any processing
    consume_loop()