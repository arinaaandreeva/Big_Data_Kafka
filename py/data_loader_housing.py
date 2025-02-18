import json
import time
import random
from confluent_kafka import Producer
import threading
import pandas as pd

# Функция для отправки сообщений
def start_producer(producer_id, topic, dataset, num_messages):
#     conf = {
#     'bootstrap.servers': 'localhost:9095,localhost:9096',  # Подключение к обоим брокерам
#     'broker.address.family': 'v4'  # Использовать только IPv4
# }
    conf = {'bootstrap.servers': 'localhost:9095'}
    producer = Producer(conf)

    for i in range(num_messages):
        if i >= len(dataset):  # Если данные закончились, выходим
            break

        sample_x = dataset[i]  # Получаем данные
        message = json.dumps(sample_x)  # Преобразуем в JSON

        # Отправка сообщения в Kafka
        producer.produce(topic, key=str(producer_id), value=message)
        producer.flush()

        # Вывод информации о отправленном сообщении
        print(f"Producer {producer_id} sent: {message}")

        # Случайная задержка
        delay = random.uniform(0.5, 2.0)  # Задержка от 0.5 до 2 секунд
        time.sleep(delay)

# Функция для загрузки данных из CSV
def load_dataset():
    # Чтение CSV-файла с помощью pandas
    df = pd.read_csv('housing.csv')
    # Преобразование данных в список словарей
    dataset = df.to_dict(orient='records')
    return dataset

# Основная функция
def main():
    topic = 'data_producer'  # Топик Kafka
    num_producers = 3  # Количество Producer'ов
    num_messages = 100  # Количество сообщений, которые отправит каждый Producer

    # Загрузка данных из CSV
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
