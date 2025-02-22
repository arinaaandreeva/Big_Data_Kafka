import json
import time
import random
from confluent_kafka import Producer
import threading
import pandas as pd


def start_producer(producer_id, topic, dataset, num_messages):
    conf = {'bootstrap.servers': 'localhost:9095,localhost:9096'}
    producer = Producer(conf)

    for i in range(num_messages):
        if i >= len(dataset):  # Если данные закончились, выходим
            break
        sample_x = dataset[i] 
        message = json.dumps(sample_x)
        
        producer.produce(topic, key=str(producer_id), value=message)# Отправка сообщения в Kafka
        producer.flush()
        print(f"Producer {producer_id} sent: {message}")

        # Случайная задержка
        delay = random.uniform(0.2, 1.0)  # Задержка от 0.2 до 1 секунд
        time.sleep(delay)
        # постоянная задержка через time.sleep(1)

# Load data
def load_dataset():
    df = pd.read_csv('housing.csv', dtype={"longitude": 'float', "latitude": 'float', 
                                           "housing_median_age": 'float', "total_rooms": 'float', 
                                           "total_bedrooms": 'float', "population": 'float', "households": 'float', 
                                           "median_income": 'float', "median_house_value": 'float', "ocean_proximity": 'str'})
    # df = df.sample(frac = 0.3)
    dataset = df.to_dict(orient='records')
    return dataset


def main():
    topic = 'data_producer'  # Топик
    num_producers = 3  # Кол-во Producer'ов
    num_messages = 100  # Кол-во сообщений, которые отправит каждый Producer

    dataset = load_dataset()

    # Запуск нескольких Producer'ов
    threads = []
    for i in range(num_producers):
        thread = threading.Thread(
            target=start_producer,
            args=(i + 1, topic, dataset, num_messages)
        )
        threads.append(thread)
        thread.start()

    # Ожидание завершения всех Producer'ов
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()