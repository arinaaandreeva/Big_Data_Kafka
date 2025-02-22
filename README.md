# Лаб 1 Kafka (Задача предсказния цен на недвижимость)

Проект систему для определения цены на недвижимость по популярному датасету [California Housing Prices](https://www.kaggle.com/datasets/camnugent/california-housing-prices) на kaggle.
Данные передаются через Apache Kafka, обрабатываются и моделируются, а результаты визуализируются с помощью Streamlit.  



#### Краткое описание файлов:
 - housing.csv исходный файл с данным
   
 1. docker-compose up -d
     в yaml указаны 2 брокера
 3. data_loader_housing.py 
      загружает данные из csv с помощью 3 producers
 4. model_training.py
     предобработка данных, стандартизация, обучение линейной регрессии. 2 Producers: Первый Producer отправляет результаты обучения в топик model_results, второй - метрики в топик metrics 
 7. Визуализация streamlit run streamlit_visualize.py
    графики RMSE, последних предсказаний и реальных данных, распределения таргета (папка img)

