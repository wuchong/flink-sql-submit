-- test temporal table join with mysql
CREATE TABLE kafka_json_source (
    rowtime TIMESTAMP,
    user_name VARCHAR,
    event ROW<message_type VARCHAR, message VARCHAR>
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'test-json',
    'connector.startup-mode' = 'earliest-offset',
    'connector.properties.0.key' = 'zookeeper.connect',
    'connector.properties.0.value' = 'localhost:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = 'localhost:9092',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);

CREATE TABLE user_dim (
    user_name VARCHAR,
    user_id BIGINT,
    city VARCHAR
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink-test',
    'connector.table' = 'user_dim',
    'connector.username' = 'root',
    'connector.password' = '123456'
);

CREATE TABLE kafka_enriched_sink (
    rowtime TIMESTAMP,
    user_name VARCHAR,
    user_id BIGINT,
    city VARCHAR,
    event ROW<message_type VARCHAR, message VARCHAR>
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'test-enriched',
    'connector.properties.0.key' = 'zookeeper.connect',
    'connector.properties.0.value' = 'localhost:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = 'localhost:9092',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);

INSERT INTO kafka_enriched_sink
SELECT
    s.rowtime, s.user_name, u.user_id, u.city, s.event
FROM (SELECT *, PROCTIME() as proctime FROM kafka_json_source) as s
LEFT JOIN user_dim FOR SYSTEM_TIME AS OF s.proctime as u -- 维表JOIN语法：必须要用 'FOR SYSTEM_TIME AS OF'
ON s.user_name = u.user_name;