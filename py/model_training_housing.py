import json
import time
import random
from confluent_kafka import Consumer, Producer
from river import compose, preprocessing, metrics, linear_model #, feature_extraction
import pandas as pd

# Фиксированные категории для One-Hot Encoding
CATEGORIES = ['NEAR BAY', '<1H OCEAN', 'INLAND', 'NEAR OCEAN', 'ISLAND']

class FixedOneHotEncoder(preprocessing.OneHotEncoder):
    def __init__(self, categories):
        super().__init__()
        self.categories = categories

    def transform_one(self, x):
        for key in x:
            if x[key] not in self.categories:
                x[key] = "UNKNOWN"  # Назначаем категорию по умолчанию
        return super().transform_one(x)

# Функция для подготовки данных
def data_preparation(sample):
    """
    Подготовка данных для модели.
    """
    sample_x = pd.DataFrame([sample])
    sample_y = sample_x.pop('median_house_value')
    return sample_x.to_dict(orient='records')[0], sample_y

# Функция для подготовки данных для визуализации
def prepare_data_for_visualizer(sample_x, y_true, y_pred, metric):
    """
    Подготовка данных для визуализации в Streamlit.
    """
    return {
        'features': sample_x,
        'true_value': y_true,
        'predicted_value': y_pred,
        'metric': metric.get()
    }

# Функция для обучения модели
def model_training():
    DATA_MINER_TOPIC = 'data_producer'
    MODEL_TRAINING_TOPIC = 'model_results'
    MODEL_TRAINING_CONSUMER_CONFIG = {'bootstrap.servers': 'localhost:9095', 'group.id': 'model_training', 'auto.offset.reset': 'earliest'}
    MODEL_TRAINING_PRODUCER_CONFIG = {'bootstrap.servers': 'localhost:9095'}
# добавить второй консьюмер не получается 
    # MODEL_TRAINING_CONSUMER_CONFIG_1 = {'bootstrap.servers': 'localhost:9095,localhost:9097', 'group.id': 'model_training', 'auto.offset.reset': 'earliest'}
    # MODEL_TRAINING_CONSUMER_CONFIG_2 = {'bootstrap.servers': 'localhost:9095,localhost:9097', 'group.id': 'model_training', 'auto.offset.reset': 'earliest'}
    # MODEL_TRAINING_PRODUCER_CONFIG = {'bootstrap.servers': 'localhost:9095,localhost:9097'}

    # consumer1 = Consumer(MODEL_TRAINING_CONSUMER_CONFIG_1)
    # consumer2 = Consumer(MODEL_TRAINING_CONSUMER_CONFIG_2)
    # consumer1.subscribe([DATA_MINER_TOPIC])
    # consumer2.subscribe([DATA_MINER_TOPIC])

    consumer = Consumer(MODEL_TRAINING_CONSUMER_CONFIG)
    producer = Producer(MODEL_TRAINING_PRODUCER_CONFIG)

    scaler = preprocessing.StandardScaler()
    one_hot_encoder = FixedOneHotEncoder(categories=CATEGORIES)

    model = compose.Pipeline(
        ('one_hot_encoder', one_hot_encoder),
        ('scaler', scaler),
        ('linear_regression', linear_model.LinearRegression())
    )

    metric = metrics.MAE()

    while True:
        message = consumer.poll(1000)
        if message is not None and message.value() is not None:
            try:
                sample = json.loads(message.value().decode('utf-8'))
                sample_x, sample_y = data_preparation(sample)

                # One-Hot Encoding с фиксированными категориями
                sample_x_transformed = model['one_hot_encoder'].transform_one(sample_x)
                sample_x_transformed = model['scaler'].transform_one(sample_x_transformed)
                y_pred = model['linear_regression'].predict_one(sample_x_transformed)
                model['linear_regression'].learn_one(sample_x_transformed, sample_y)

                metric.update(sample_y, y_pred)
                to_visualizer = prepare_data_for_visualizer(sample_x, sample_y, y_pred, metric)
                print(to_visualizer)

                producer.produce(
                    MODEL_TRAINING_TOPIC, key='1',
                    value=json.dumps(to_visualizer)
                )
                producer.flush()
                #time.sleep(1)
                delay = random.uniform(0.5, 2.0)  # Задержка от 0.5 до 2 секунд
                time.sleep(delay)

            except json.JSONDecodeError as e:
                print(f"Ошибка JSON: {e}")


if __name__ == "__main__":
    model_training()
