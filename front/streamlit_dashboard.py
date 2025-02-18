import streamlit as st
import pandas as pd
import json
from confluent_kafka import Consumer

# Функция для получения данных из Kafka
def get_data_from_kafka():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9095',
        'group.id': 'streamlit_visualizer',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['model_results'])

    data = []
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            break
        if msg.error():
            st.error(f"Ошибка: {msg.error()}")
            continue
        data.append(json.loads(msg.value().decode('utf-8')))
    return data

# Основной код Streamlit
def main():
    st.title("California Housing Prices Prediction")

    # Получение данных из Kafka
    data = get_data_from_kafka()

    if data:
        # Преобразование данных в DataFrame
        df = pd.DataFrame(data)
        
        # Отображение данных
        st.write("### Последние предсказания")
        st.dataframe(df.tail())

        # График истинных и предсказанных значений
        st.write("### График истинных и предсказанных значений")
        st.line_chart(df[['true_value', 'predicted_value']])

        # Отображение метрики
        st.write("### Текущее значение метрики MAE")
        st.write(df['metric'].iloc[-1])

if __name__ == "__main__":
    main()
