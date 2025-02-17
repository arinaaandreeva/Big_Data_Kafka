import json
import config

import streamlit as st
import matplotlib.pyplot as plt
import json
from confluent_kafka import Consumer


# Фичи, которые будем хранить в сессии
features = ['Date_', 'Return', 'metric']


def create_session_states():
    """Создаем списки для хранения данных в сессии Streamlit"""
    for feature in features:
        if feature not in st.session_state:
            st.session_state[feature] = []


def add_values_to_session_state(sample):
    """Добавляем новые данные в историю сессии"""
    for feature in features:
        st.session_state[feature].append(sample[feature])


def draw_return_dashboard(return_chart):
    """График доходности во времени"""
    return_chart.line_chart(
        {'Return': st.session_state['Return']}, 
        x=st.session_state['Date_'],
        use_container_width=True
    )


def draw_metric_dashboard(metric_chart):
    """График изменения метрики во времени"""
    metric_chart.line_chart(
        {'metric': st.session_state['metric']}, 
        x=st.session_state['Date_'],
        use_container_width=True
    )


def draw_return_histogram(hist_chart):
    """Гистограмма доходности"""
    fig, ax = plt.subplots()
    ax.hist(st.session_state['Return'], bins=20)
    ax.set_xlabel('Return')
    ax.set_ylabel('Frequency')
    hist_chart.pyplot(fig)
    plt.close()


def visualizer():
    create_session_states()
    VISUALIZER_CONSUMER_CONFIG = {'bootstrap.servers': 'localhost:9095', 'group.id': 'visualizer'}
    MODEL_TRAINING_TOPIC = {'bootstrap.servers': 'localhost:9095', 'group.id': 'model_training'}
    consumer = Consumer(VISUALIZER_CONSUMER_CONFIG)
    consumer.subscribe([MODEL_TRAINING_TOPIC])

    # Создаем контейнеры для визуализации
    return_chart = st.container(border=True)
    return_chart.title('Stock Return over Time')
    return_chart = return_chart.empty()

    metric_chart = st.container(border=True)
    metric_chart.title('Metric over Time')
    metric_chart = metric_chart.empty()

    hist_chart = st.container(border=True)
    hist_chart.title('Stock Return Distribution')
    hist_chart = hist_chart.empty()

    while True:
        message = consumer.poll(1000)
        if message is not None:
            sample = json.loads(message.value().decode('utf-8'))
            print(sample)
            add_values_to_session_state(sample)

            draw_return_dashboard(return_chart)
            draw_metric_dashboard(metric_chart)
            draw_return_histogram(hist_chart)


if __name__ == "__main__":
    visualizer()
