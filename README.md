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
    
    ```

6. Создать docker network:
    ```bash
    sudo docker network create kafka-network
    ```

7. Запустить сервисы:
    ```bash
    sudo docker compose up zookeeper kafka_1 kafka_2 kafka_3 kafka-ui -d
    ```
    При первом запуске до создания топика программу **app** запускать не надо. После создания топика при последуюший перезапусках можно использовать
    ```bash
    sudo docker compose up -d
    ```

8. Создать нужные топики:
    ```bash
    sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic filtered_messages --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092 && sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic blocked_users --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092 && sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic messages --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092
    ```

9. Спуститься обратно в директорию kafka2/ и оздать рабочее окружение и активировать его:
    ```bash
    cd ../
    python3 -m venv venv
    source venv/bin/activate
    ```
10. Установить зависимости:
    ```bash
    pip install -r requirements.txt
    ```

11. Запустить программу для сортировки и цензуры сообщений:
    ```bash
    faust -A kafka_streams worker -l INFO
    ```

12. В другом окне терминала добавить запрещенные слова:

    ```bash
    echo '{"words": ["loh", "durak", "chert"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic bad_words
    ```

    Списки можно менять, это просто пример.

13. Добавить список блокировок:
    ```bash
    echo '{"blocker":"clown", "blocked":["dodik", "spammer"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"spammer", "blocked":[]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"dodik", "blocked":["spammer", "payaso"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"payaso", "blocked":["spammer"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    ```

    Списки можно менять, это просто пример.


14. Проверить фильтрацию сообщений:

    ```bash
    echo '{"sender_id":228,"sender_name":"clown","recipient_id":69,"recipient_name":"dodik","amount":1.75,"content":"loh"}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic messages
    echo '{"sender_id":228,"sender_name":"dodik","recipient_id":69,"recipient_name":"payaso","amount":1.75,"content":"durak"}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic messages
    echo '{"sender_id":228,"sender_name":"payaso","recipient_id":69,"recipient_name":"spammer","amount":1.75,"content":"chert"}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic messages
    echo '{"sender_id":228,"sender_name":"clown","recipient_id":69,"recipient_name":"dodik","amount":1.75,"content":"labubu"}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic messages
    ```

    Ожидаемый результат: три первый сообщения будут зацензурированы, четвертое - нет.

15. Запустить генератор сообщений.
    ```bash
    sudo docker compose up app -d
    ```

16. Проверить работу блокировок из пункта 13, открыв топик filtered_messages. Сообщения от отправителя spammer не доходят никому, до получателя spammer доходят сообщения от всех отправителей.


## Автор
[Aliaksei Tulko](https://github.com/aleksej-tulko)