from confluent_kafka import Consumer
import streamlit as st
import json
import pandas as pd
import time
import matplotlib.pyplot as plt

st.set_page_config(page_title="House Price Prediction", layout="wide")

# Инициализация состояния сессии
if "RMSE" not in st.session_state:
    st.session_state["RMSE"] = []
if "true_values" not in st.session_state:
    st.session_state["true_values"] = []
if "predicted_values" not in st.session_state:
    st.session_state["predicted_values"] = []

# топик model_results
bootstrap_servers = "localhost:9095"
model_results_consumer = Consumer({"bootstrap.servers": bootstrap_servers,
                                    "group.id": "streamlit_model_consumer",
                                    "auto.offset.reset": "earliest"})


model_results_consumer.subscribe(["model_results"])

# топик metrics
metrics_consumer = Consumer({"bootstrap.servers": bootstrap_servers,
                            "group.id": "streamlit_metrics_consumer",
                            "auto.offset.reset": "earliest"})
metrics_consumer.subscribe(["metrics"])

st.header("House Price Prediction Dashboard on Kaggle Dataset (https://www.kaggle.com/datasets/camnugent/california-housing-prices)")

# Контейнеры
st.subheader("Root Mean Squared Error (RMSE)")
chart_holder_rmse = st.empty()

st.subheader("Реальные и предсказанные значения")
predictions_chart_holder = st.empty()

st.subheader("Последние предсказания")
predictions_table_holder = st.empty()

st.subheader("Распределение реального таргета")
target_distribution_holder = st.empty()


while True:
    # Чтение из model_results
    model_message = model_results_consumer.poll(1.0)
    if model_message is not None:
        try:
            model_data = json.loads(model_message.value().decode("utf-8"))
            true_value = model_data.get("true_value")
            predicted_value = model_data.get("predicted_value")
            if true_value is not None and predicted_value is not None:

                st.session_state["true_values"].append(true_value)
                st.session_state["predicted_values"].append(predicted_value)

                # Показываем последние 50 значений
                if len(st.session_state["true_values"]) > 50:
                    st.session_state["true_values"] = st.session_state["true_values"][-50:]
                    st.session_state["predicted_values"] = st.session_state["predicted_values"][-50:]

                predictions_df = pd.DataFrame({
                    "Реальное значение": st.session_state["true_values"],
                    "Предсказанное значение": st.session_state["predicted_values"]
                })


                predictions_chart_holder.line_chart(predictions_df)
                predictions_table_holder.dataframe(predictions_df.tail(10))  # Показываем последние 10 значений

                # Обновляем график распределения реального таргета
                if st.session_state["true_values"]:
                    fig, ax = plt.subplots()
                    ax.hist(st.session_state["true_values"], bins=20, color='blue', alpha=0.7)
                    ax.set_title("Распределение реального таргета")
                    ax.set_xlabel("Цена дома")
                    ax.set_ylabel("Частота")
                    target_distribution_holder.pyplot(fig)
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON: {e}")

    # Чтение данных из топика metrics
    metrics_message = metrics_consumer.poll(1.0)
    if metrics_message is not None:
        try:
            metrics_data = json.loads(metrics_message.value().decode("utf-8"))
            # print(f"Получено сообщение из топика metrics: {metrics_data}")
            rmse_value = metrics_data.get("RMSE")
            if rmse_value is not None:
                st.session_state["RMSE"].append(rmse_value)
                chart_holder_rmse.line_chart(st.session_state["RMSE"])
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON: {e}")

    # задержка
    time.sleep(0.1)