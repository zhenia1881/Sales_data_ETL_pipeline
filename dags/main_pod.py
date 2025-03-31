from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum, avg, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.window import Window
from faker import Faker
import random
import psycopg2
import logging

def init_spark():
    return SparkSession.builder \
        .appName("AirflowSpark") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_data_pipeline_prod',
    default_args=default_args,
    description='Стабильная версия пайплайна',
    schedule_interval='45 12 * * 2',
    catchup=False,
)

def create_postgres_tables():
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales (
                    sale_id INT PRIMARY KEY,
                    customer_id INT,
                    product_id INT,
                    quantity INT,
                    sale_date DATE,
                    sale_amount DOUBLE PRECISION,
                    region VARCHAR(20)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales_aggregated (
                    region VARCHAR(20),
                    product_id INT,
                    total_quantity BIGINT,
                    total_amount DOUBLE PRECISION,
                    average_sale_amount DOUBLE PRECISION,
                    aggregation_date TIMESTAMP
                )
            """)
        logging.info("Tables created successfully")
    except Exception as e:
        logging.error(f"Error creating tables: {str(e)}")
        raise
    finally:
        conn.close()

def generate_sales_data(**kwargs):
    spark = init_spark()
    try:
        schema = StructType([
            StructField("sale_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("sale_date", DateType(), False),
            StructField("sale_amount", DoubleType(), False),
            StructField("region", StringType(), False)
        ])

        fake = Faker()
        data = [
            (
                i + 1,
                random.randint(1, 100000),
                random.randint(1, 1000),
                random.randint(1, 10),
                fake.date_between(start_date='-1y', end_date='today'),
                round(random.uniform(10, 1000), 2),
                random.choice(['North', 'South', 'East', 'West'])
            ) for i in range(1000000)
        ]

        df = spark.createDataFrame(data, schema)
        output_path = "/opt/airflow/data/sales_raw"
        df.write.mode("overwrite").parquet(output_path)
        return output_path
    finally:
        spark.stop()

def clean_data(**kwargs):
    ti = kwargs['ti']
    input_path = ti.xcom_pull(task_ids='generate_sales_data')

    spark = init_spark()
    try:
        df = spark.read.parquet(input_path)
        df_clean = df.dropDuplicates(["sale_id"]) \
            .withColumn("sale_date", date_format(col("sale_date"), "yyyy-MM-dd"))

        output_path = "/opt/airflow/data/sales_clean"
        df_clean.write.mode("overwrite").parquet(output_path)
        return output_path
    finally:
        spark.stop()

def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    input_path = ti.xcom_pull(task_ids='clean_data')

    spark = init_spark()
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales (
                    sale_id INT PRIMARY KEY,
                    customer_id INT,
                    product_id INT,
                    quantity INT,
                    sale_date DATE,
                    sale_amount DOUBLE PRECISION,
                    region VARCHAR(20)
                )
            """)

        df = spark.read.parquet(input_path)
        records = df.collect()

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_sales AS
                SELECT * FROM sales WITH NO DATA
            """)

            for row in records:
                cur.execute("""
                    INSERT INTO temp_sales VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))

            cur.execute("""
                INSERT INTO sales
                SELECT * FROM temp_sales
                WHERE NOT EXISTS (
                    SELECT 1 FROM sales 
                    WHERE sales.sale_id = temp_sales.sale_id
                )
            """)

            inserted_rows = cur.rowcount
            logging.info(f"Inserted {inserted_rows} new records")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading to PostgreSQL: {str(e)}")
        raise
    finally:
        spark.stop()
        conn.close()

def aggregate_data(**kwargs):
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sales_aggregated
                SELECT 
                    region,
                    product_id,
                    SUM(quantity) as total_quantity,
                    SUM(sale_amount) as total_amount,
                    AVG(sale_amount) as average_sale_amount,
                    NOW() as aggregation_date
                FROM sales
                GROUP BY region, product_id
            """)
        conn.commit()
    finally:
        conn.close()


def load_to_clickhouse(**kwargs):
    from clickhouse_driver import Client
    from psycopg2 import connect
    import logging
    from tenacity import retry, stop_after_attempt, wait_exponential
    import time

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("=== Начало загрузки данных в ClickHouse ===")

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=4, max=10),
           reraise=True,
           before=lambda *args: logger.warning("Повторная попытка вставки..."))
    def safe_insert(client, query, data):
        start_time = time.time()
        try:
            rows = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in data]
            result = client.execute(query, rows)
            logger.info(f"Вставлено {len(data)} строк за {time.time() - start_time:.2f} сек")
            return result
        except Exception as e:
            logger.error(f"Ошибка вставки: {str(e)}")
            raise

    try:
        logger.info("Устанавливаю подключение к PostgreSQL...")
        pg_conn = None
        try:
            pg_conn = connect(
                dbname="airflow",
                user="airflow",
                password="airflow",
                host="postgres",
                port="5432",
                connect_timeout=10
            )
            logger.info("Подключение к PostgreSQL успешно")
        except Exception as pg_err:
            logger.error(f"Ошибка подключения к PostgreSQL: {str(pg_err)}")
            raise

        logger.info("Устанавливаю подключение к ClickHouse...")
        ch_client = None
        try:
            ch_client = Client(
                host='clickhouse',
                port=9000,
                user='airflow',
                password='airflow',
                database='default',
                connect_timeout=30,
                settings={
                    'connect_timeout': 30,
                    'send_receive_timeout': 600,  # 10 минут
                    'max_block_size': 100000,
                    'use_numpy': False
                }
            )
            ch_client.execute('SELECT 1')
            logger.info("Подключение к ClickHouse успешно")
        except Exception as ch_err:
            logger.error(f"Ошибка подключения к ClickHouse: {str(ch_err)}")
            raise

        try:
            logger.info("Проверяю существование таблицы sales_aggregated...")
            ch_client.execute("""
                CREATE TABLE IF NOT EXISTS sales_aggregated (
                    region String,
                    product_id Int32,
                    total_quantity Int64,
                    total_amount Float64,
                    average_sale_amount Float64,
                    aggregation_date DateTime
                ) ENGINE = MergeTree()
                ORDER BY region
            """)
        except Exception as table_err:
            logger.error(f"Ошибка создания таблицы: {str(table_err)}")
            raise

        try:
            logger.info("Получаю данные из PostgreSQL...")
            with pg_conn.cursor() as pg_cur:
                # Сначала проверяем количество строк
                pg_cur.execute("SELECT COUNT(*) FROM sales_aggregated")
                count = pg_cur.fetchone()[0]
                logger.info(f"Найдено {count} строк для загрузки")

                if count == 0:
                    logger.warning("Нет данных для загрузки")
                    return False

                TEST_MODE = False
                limit = " LIMIT 100" if TEST_MODE else ""
                pg_cur.execute(f"SELECT * FROM sales_aggregated{limit}")
                data = pg_cur.fetchall()
                logger.info(f"Получено {len(data)} строк из PostgreSQL")

                BATCH_SIZE = 50000
                total_inserted = 0

                for i in range(0, len(data), BATCH_SIZE):
                    batch = data[i:i + BATCH_SIZE]
                    batch_num = (i // BATCH_SIZE) + 1
                    total_batches = (len(data) - 1) // BATCH_SIZE + 1

                    logger.info(f"Обработка пакета {batch_num}/{total_batches} ({len(batch)} строк)")

                    try:
                        safe_insert(ch_client, "INSERT INTO sales_aggregated VALUES", batch)
                        total_inserted += len(batch)
                    except Exception as insert_err:
                        logger.error(f"Ошибка вставки пакета {batch_num}: {str(insert_err)}")
                        raise

                logger.info(f"Успешно загружено {total_inserted} строк из {len(data)}")
                return total_inserted == len(data)

        except Exception as data_err:
            logger.error(f"Ошибка обработки данных: {str(data_err)}")
            raise

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
        raise

    finally:
        logger.info("Завершение работы, освобождение ресурсов...")
        try:
            if pg_conn:
                pg_conn.close()
                logger.info("Подключение к PostgreSQL закрыто")
        except Exception as e:
            logger.warning(f"Ошибка при закрытии PostgreSQL: {str(e)}")

        try:
            if ch_client:
                ch_client.disconnect()
                logger.info("Подключение к ClickHouse закрыто")
        except Exception as e:
            logger.warning(f"Ошибка при закрытии ClickHouse: {str(e)}")

        logger.info("=== Завершение загрузки данных ===")

create_tables_task = PythonOperator(
    task_id='create_postgres_tables',
    python_callable=create_postgres_tables,
    dag=dag,
)

generate_task = PythonOperator(
    task_id='generate_sales_data',
    python_callable=generate_sales_data,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='aggregate_data',
    python_callable=aggregate_data,
    dag=dag,
)

clickhouse_task = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    dag=dag,
)

create_tables_task >> generate_task >> clean_task >> load_task >> aggregate_task >> clickhouse_task