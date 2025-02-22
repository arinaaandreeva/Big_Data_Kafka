import json
import time
import random
from confluent_kafka import Consumer, Producer
from river import compose, preprocessing, metrics, linear_model
import pandas as pd

# предобработка
# Словарь для замены категорий на числа, ранжируем по крутости
CATEGORY_MAPPING = {
    'NEAR BAY': 3,
    '<1H OCEAN': 2,
    'INLAND': 1,
    'NEAR OCEAN': 4,
    'ISLAND': 5
}

def data_preparation(sample):
    sample_x = pd.DataFrame([sample])
    sample_y = float(sample_x.pop('median_house_value'))
    # Заменяем категории на числа
    for key in sample_x.columns:
        if key in CATEGORY_MAPPING:
            sample_x[key] = sample_x[key].map(lambda x: CATEGORY_MAPPING.get(x.strip(), 0)).astype(float)
        else:
            sample_x[key] = pd.to_numeric(sample_x[key], errors='coerce')
    sample_x = sample_x.to_dict(orient='records')[0]
    return sample_x, sample_y

# Функция для подготовки данных для визуализации
def prepare_data_for_visualizer(sample_x, y_true, y_pred, metric):
    return {
        'true_value': y_true,
        'predicted_value': y_pred,
        'metric': metric.get()
    }

if __name__ == "__main__":
    # Создаем Consumer и Producer'ы
    DATA_MINER_TOPIC = 'data_producer'
    MODEL_TRAINING_TOPIC = 'model_results'
    METRICS_TOPIC = 'metrics'

    MODEL_TRAINING_CONSUMER_CONFIG = {
        'bootstrap.servers': 'localhost:9096',
        'group.id': 'model_training',
        'auto.offset.reset': 'earliest'
    }
    MODEL_TRAINING_PRODUCER_CONFIG = {'bootstrap.servers': 'localhost:9095'}
    METRICS_PRODUCER_CONFIG = {'bootstrap.servers': 'localhost:9095'}

    consumer = Consumer(MODEL_TRAINING_CONSUMER_CONFIG)
    training_producer = Producer(MODEL_TRAINING_PRODUCER_CONFIG)
    metrics_producer = Producer(METRICS_PRODUCER_CONFIG)

    consumer.subscribe([DATA_MINER_TOPIC])
    # Инициализация модели и скалера
    scaler = preprocessing.StandardScaler()

    model = compose.Pipeline(
        ('scaler', scaler),
        ('linear_regression', linear_model.LinearRegression())
    )

    metric = metrics.RMSE()

    while True:
        message = consumer.poll(1000)
        if message is None:
            continue
        message_value = message.value().decode('utf-8')

        try:
            sample = json.loads(message_value)
            sample_x, sample_y = data_preparation(sample)

            # Обучение модели
            sample_x_transformed = model['scaler'].transform_one(sample_x)
            model['linear_regression'].learn_one(sample_x_transformed, sample_y)
            y_pred = model['linear_regression'].predict_one(sample_x_transformed)
            metric.update(sample_y, y_pred)

            to_visualizer = prepare_data_for_visualizer(sample_x, sample_y, y_pred, metric)
            print(to_visualizer)

            # Отправка результатов обучения в топик model_results
            training_producer.produce(
                MODEL_TRAINING_TOPIC, key='1',
                value=json.dumps(to_visualizer)
            )
            training_producer.flush()

            # Отправка метрик в топик metrics
            metrics_producer.produce(
                METRICS_TOPIC, key='1',
                value=json.dumps({"RMSE": metric.get()})
            )
            metrics_producer.flush()

            # Случайная задержка
            delay = random.uniform(0.2, 1.0)
            time.sleep(delay)

        except json.JSONDecodeError as e:
            print(f"Ошибка JSON: {e}")
            print(f"Содержимое сообщения: {message_value}")
