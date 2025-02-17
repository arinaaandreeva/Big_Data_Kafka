import time
import json
import config

from confluent_kafka import Producer
from river import stream


def data_loader():
    DATA_MINER_PRODUCER_CONFIG = {'bootstrap.servers': 'localhost:9095'}
    producer = Producer(DATA_MINER_PRODUCER_CONFIG)

    dateset_size = 20000
    dataset_stream = stream.iter_csv(
        'NVIDIA_STOCK.csv',
        converters={
            'Price': str,
            'Adj Close': float,
            'Close': float,
            'High': float,
            'Low': float,
            'Open': float,
            'Volume': int
        }
    )
    DATA_MINER_TOPIC = 'data_producer'
    for _ in range(dateset_size):
        sample_x, sample_y = next(dataset_stream)
        producer.produce(DATA_MINER_TOPIC, key='1',
                         value=json.dumps(sample_x))
        producer.flush()
        print(sample_x)
        time.sleep(1)


if __name__ == "__main__":
    data_loader()
