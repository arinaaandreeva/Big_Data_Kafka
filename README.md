# Лаб 1 Kafka (Задача предсказния цен на недвижимость)

Проект систему для определения цены на недвижимость по популярному датасету [California Housing Prices](https://www.kaggle.com/datasets/camnugent/california-housing-prices) на kaggle. Данные передаются через Apache Kafka, обрабатываются и моделируются, а результаты визуализируются с помощью Streamlit.  

 - housing.csv исходный файл с данным
Для запуска:
 1. docker-compose up -d
 2. Загрузка данных, предобработка, обучение модели data_loader_housing.py, model_training_housing.py
 3. Визуализация streamlit run streamlit_dashboard.py
