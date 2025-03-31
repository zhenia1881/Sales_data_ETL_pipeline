# Пайплайн обработки данных о продажах

![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=ClickHouse&logoColor=black)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=Apache%20Spark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## 🔧 Основные технологии

- **Оркестрация**: Apache Airflow 2.5+
- **Базы данных**:
  - PostgreSQL 13+ (транзакционные данные)
  - ClickHouse 22+ (аналитические данные)
- **Обработка данных**: PySpark 3.3+
- **Контейнеризация**: Docker 20+, Docker Compose 1.29+

## 📌 Основной функционал

- 🏗 **Генерация данных**: 1 млн реалистичных записей о продажах
- 🧹 **Очистка данных**: дедупликация, валидация, преобразование
- 🗄 **Хранение**: двухуровневая архитектура (PostgreSQL + ClickHouse)
- 📊 **Аналитика**: агрегация по регионам/товарам, расчет среднего чека
- ⏰ **Расписание**: автоматический запуск по вторникам в 12:45 МСК

## 🚀 Запуск

### ⚙️ Требования к системе

- Docker версии 20.10 или выше
- Docker Compose версии 1.29 или выше
- Рекомендуется 8 ГБ оперативной памяти
- 10 ГБ свободного места на диске

### 🛠 Установка

1. Клонируйте репозиторий проекта:
```bash
git clone https://github.com/zhenia1881/Sales_data_ETL_pipeline.git
cd Sales_data_pipeline
```
2. Запустите все сервисы в фоновом режиме:
```bash
docker-compose up -d
```

3. Дождитесь инициализации всех компонентов (2-3 минуты)
4. Откройте веб-интерфейс Airflow:
```
http://localhost:8080
```

Логин: airflow
Пароль: airflow
