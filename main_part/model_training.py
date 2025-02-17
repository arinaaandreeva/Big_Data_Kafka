import json
import time
from datetime import datetime
from river import compose, preprocessing, metrics, ensemble
from river import metrics, time_series, feature_extraction, preprocessing, neural_net, optim
from confluent_kafka import Consumer, Producer


def data_preparation(x):
    x = x.rename(columns={'Price': 'Date_'}) # перепутаны обозначения в kaggle dataset
    x['Date_'] = datetime.strptime(x['Date_'], "%Y-%m-%d %H:%M:%S")
    # Вычисляем доходность (процентное изменение цены)
    x['Return'] = x['Adj Close'].pct_change()
    # x['Return_lag'] = x['Return'].shift(1)
    x = x.dropna()
    y = x.pop('Return')
    # x = x.select_dtypes(include=['float64', 'int64'])
    x.drop(columns=['Adj Close', 'Return'], inplace=True)
    x.set_index('Date_', inplace = True)
    return x, y


def prepare_data_for_visualizer(sample_x, metric):
    return {'Date_': sample_x['Date_'], 'Return': sample_x['Return'],
            'metric': metric}


# Подготовка данных (должна быть определена)
def data_preparation(x):
    x = x.rename(columns={'Price': 'Date_'})  # Исправляем ошибку в Kaggle dataset
    x['Date_'] = datetime.strptime(x['Date_'], "%Y-%m-%d %H:%M:%S")
    x['Return'] = x['Adj Close'].pct_change()
    x = x.dropna()
    y = x.pop('Return')
    x.set_index('Date_', inplace=True)
    return x, y

# # Функция обучения
# def model_training():
#     DATA_MINER_TOPIC = 'data_producer'
#     MODEL_TRAINING_TOPIC = 'model_results'
#     MODEL_TRAINING_CONSUMER_CONFIG = {'bootstrap.servers': 'localhost:9095', 'group.id': 'model_training'}
#     MODEL_TRAINING_PRODUCER_CONFIG = {'bootstrap.servers': 'localhost:9095'}

#     consumer = Consumer(MODEL_TRAINING_CONSUMER_CONFIG)
#     consumer.subscribe([DATA_MINER_TOPIC])

#     producer = Producer(MODEL_TRAINING_PRODUCER_CONFIG)

#     # Стандартная нормализация
#     scaler = preprocessing.StandardScaler()
#     # MLPRegressor с 2 скрытыми слоями (64 → 32)
#     model = neural_net.MLPRegressor(hidden_dims=(64, 32),  activations=("relu", "relu"), optimizer=optim.Adam(0.01))
#     pipeline = compose.Pipeline(scaler, model)
#     metric = metrics.MAE()

#     batch_size = 100
#     batch = []

#     while True:
#         message = consumer.poll(1000)
#         if message is not None:
#             sample = json.loads(message.value().decode('utf-8'))
#             sample_x, sample_y = data_preparation(sample)
            
#             batch.append((sample_x, sample_y))

#             if len(batch) >= batch_size:  # Когда накопили 10 примеров, обучаем модель
#                 for x, y in batch:
#                     pipeline.learn_one(x, y)
            
#                     y_pred = pipeline.predict_one(x)

#                     # Обновляем метрику
#                     metric.update(y, y_pred)

#                     to_visualizer = prepare_data_for_visualizer(sample_x, metric.get())
#                     batch = []  # Очищаем batch
#                     # # Подготовка данных для визуализации
#                     # to_visualizer = {
#                     #     "Date_": sample_x.index[-1],  # Последняя дата
#                     #     "true": sample_y,
#                     #     "predicted": y_pred,
#                     #     "mae": metric.get()
#                     # }
                    
#                     print(to_visualizer)
#                     producer.produce(
#                         MODEL_TRAINING_TOPIC, key='1',
#                         value=json.dumps(to_visualizer)
#                     )
#                     producer.flush()

#                     time.sleep(1)


# if __name__ == "__main__":
#     model_training()



def model_training():
    DATA_MINER_TOPIC = 'data_producer'
    MODEL_TRAINING_TOPIC = 'model_results'
    MODEL_TRAINING_CONSUMER_CONFIG = {'bootstrap.servers': 'localhost:9095', 'group.id': 'model_training'}
    MODEL_TRAINING_PRODUCER_CONFIG = {'bootstrap.servers': 'localhost:9095'}

    consumer = Consumer(MODEL_TRAINING_CONSUMER_CONFIG)
    consumer.subscribe([DATA_MINER_TOPIC])
    producer = Producer(MODEL_TRAINING_PRODUCER_CONFIG)

    # Нормализация
    scaler = preprocessing.StandardScaler()
    
    # Модель временных рядов
    model = time_series.SNARIMAX(p=1, d=0, q=1, m=1)
    
    metric = metrics.MAE()
    
    batch_size = 100  # Размер батча
    batch_data = []  # Буфер для батча

    while True:
        message = consumer.poll(1000)
        if message is not None and message.value() is not None:
            try:
                sample = json.loads(message.value().decode('utf-8'))
                sample_x, sample_y = data_preparation(sample)

                # Накопление данных в батч
                batch_data.append((sample_x, sample_y))

                # Если батч собран — обучаем модель
                if len(batch_data) >= batch_size:
                    for x, y in batch_data:
                        x_transformed = scaler.transform_one(x)
                        model.learn_one(x_transformed, y)

                    # Очищаем буфер
                    batch_data = []

                # Делаем прогноз
                sample_x_transformed = scaler.transform_one(sample_x)
                y_pred = model.predict_one(sample_x_transformed)

                # Обновляем метрику
                metric.update(sample_y, y_pred)

                # Подготовка данных для визуализации
                to_visualizer = prepare_data_for_visualizer(sample_x, metric.get())
                print(to_visualizer)

                producer.produce(
                    MODEL_TRAINING_TOPIC, key='1',
                    value=json.dumps(to_visualizer)
                )
                producer.flush()
                time.sleep(1)

            except json.JSONDecodeError as e:
                print(f"Ошибка JSON: {e}")

if __name__ == "__main__":
    model_training()
