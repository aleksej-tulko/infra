# Описание

consumer проверяет вхождения в тадблицы Postgres *online.orders* и *online.users* и логгирует новые записи в стандартный вывод.

Отправка данных из БД в Kafka осуществляется при помощь Kafka Connect c Debezium коннектором. Конфигурация коннектора записана в файл *connector.json*.

Мониторинг коннектора осуществлен с помощью Prometheus и Grafana.

Kafka UI для отслеживания изменения в топика доступен по адресу *http://$server_ip:8080*.

## Требования

- **OS**: Linux Ubuntu 24.04 amd
- **Python**: Python 3.12.3
- **Docker**: 28.2.2 - https://docs.docker.com/engine/install/ubuntu/

## Подготовка к запуску

1. Склонировать репозиторий:
    ```bash
    git clone https://github.com/aleksej-tulko/infra.git
    cd infra
    ```
2. Скачать плагин для коннектора:
    ```bash
    mkdir confluent-hub-components
    curl -O https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.2.0.Final/debezium-connector-postgres-3.2.0.Final-plugin.tar.gz
    tar -xvf debezium-connector-postgres-3.2.0.Final-plugin.tar.gz
    mv debezium-connector-postgres confluent-hub-components
    ```
3. Скачать плагин для мониторинга Kafka Connect:
    ```bash
    curl -O https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.15.0/jmx_prometheus_javaagent-0.15.0.jar
    mv jmx_prometheus_javaagent-0.15.0.jar kafka-connect
    ```
4. Создать файл .env c переменными:
    ```env
    ACKS_LEVEL='all'
    AUTOOFF_RESET='earliest'
    ENABLE_AUTOCOMMIT=False
    FETCH_MIN_BYTES=200
    FETCH_WAIT_MAX_MS=1000
    SESSION_TIME_MS=6000
    POSTGRES_USER=postgres-user
    POSTGRES_PASSWORD=postgres-pw
    POSTGRES_DB=online
    ORDERS='online.public.orders'
    USERS='online.public.users'
    ```
5. Запусить сервисы:
    ```bash
    docker compose up -d
    ```

6. Создать схему в БД и добавить пользователей:
    ```bash
    docker exec -it postgres psql -h 127.0.0.1 -U postgres-user -d online

    CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );


    CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        user_id INT REFERENCES users(id),
        product_name VARCHAR(100),
        quantity INT,
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
    INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
    INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');
    INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');
    ```

7. Создать коннектор:
    ```bash
    curl -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
    ```

8. Создать заказы:
    ```bash
    docker exec -it postgres psql -h 127.0.0.1 -U postgres-user -d online

    WITH u AS (
    SELECT id, row_number() OVER (ORDER BY id) AS rn, count(*) OVER () AS c
    FROM users
    ),
    g AS (
    SELECT generate_series(1, 1000000) AS i
    )
    INSERT INTO orders (user_id, product_name, quantity)
    SELECT
    u.id,
    'Product_' || g.i,
    (g.i % 5) + 1
    FROM g
    JOIN u ON u.rn = ((g.i - 1) % u.c) + 1;
    ```

9. Создать рабочее окружение и запустить консюера
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    cd app
    python3 consumer.py
    ```

## Автор
[Aliaksei Tulko](https://github.com/aleksej-tulko)