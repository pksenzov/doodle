from time import time
from datetime import timedelta
from random import randrange
import logging
import faust


class User(faust.Record, serializer='json'):
    uid: str
    ts: int


class User_stats(faust.Record, serializer='json'):
    ts: int
    count: int
    interval: int


WINDOW = 60
WINDOW_EXPIRES = 60
STEP = 1
PARTITIONS = 1
CLEANUP_INTERVAL = 1.0
TOPIC = 'user-event'
SINK = 'user-stat-event'
TABLE = 'users'
KAFKA = 'kafka://localhost:9092'
IS_PERF_METRICS_ON = False

# Create logger
logger = logging.getLogger('app')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

app = faust.App('doodle', broker=KAFKA, topic_partitions=1)
app.conf.table_cleanup_interval = CLEANUP_INTERVAL

source = app.topic(TOPIC, value_type=User)
sink = app.topic(SINK, value_type=User_stats)


def window_processor(key, value):
    timestamp = key[1][0]
    unique_cnt = len(value)

    logger.info(
        f'Processing window:\n'
        f'Unique users per last {WINDOW} seconds: {unique_cnt}\n'
        f'Timestamp: {timestamp}')

    sink.send_soon(value=User_stats(ts=timestamp, count=unique_cnt, interval=WINDOW))


users_table = app.Table(TABLE, partitions=PARTITIONS, default=set, on_window_close=window_processor)\
    .hopping(WINDOW, STEP, expires=timedelta(seconds=WINDOW_EXPIRES))


@app.agent(source)
async def aggregate_users(users):
    async for user in users:
        users_table['uids'] |= {user.uid}

        # emit how many events are being processed every second.
        if IS_PERF_METRICS_ON:
            logger.info(app.monitor.events_s)


# producer: 62-75
@app.timer(0.1)
async def produce():
    await source.send(value=User(uid=randrange(1000), ts=int(time())))

if __name__ == '__main__':
    app.main()