-- test group aggregate from kafka and insert into mysql
CREATE TABLE kafka_json_source (
    rowtime TIMESTAMP,
    user_name VARCHAR,
    event ROW<message_type varchar, message VARCHAR>
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

CREATE TABLE pvuv_sink (
    ctime VARCHAR,
    uv BIGINT,
    pv BIGINT
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink-test',
    'connector.table' = 'pvuv_sink',
    'connector.username' = 'root',
    'connector.password' = '123456',
    'connector.write.flush.max-rows' = '1' -- 默认攒齐5000条才会写出去，为了测试每来一条就写出去
);

INSERT INTO pvuv_sink
SELECT
    date_format(rowtime, 'yyyy-MM-dd HH:00') as ctime, -- 将数据从时间戳格式（2018-12-04 15:44:54），转换为hour格式(2018-12-04 15:00)
    count(distinct user_name) as uv,
    count(user_name) as pv
FROM kafka_json_source
--按照天做聚合
GROUP BY date_format(rowtime, 'yyyy-MM-dd HH:00');