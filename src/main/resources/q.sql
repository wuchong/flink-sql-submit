-- CSV 目前有问题，还跑不通
-- 计算从0点开始，每个店铺的uv/pv
CREATE TABLE src (
    ctime TIMESTAMP,
    category_id VARCHAR,
    shop_id VARCHAR,
    uid VARCHAR
) WITH (
    'connector.type' = 'filesystem',
    'connector.path' = 'file://${work_space}/countdistinct/source.csv',
    'connector.property-version' = '1',
    'format.type' = 'csv',
    'format.property-version' = '1',
    'format.ignore-first-line' = 'true',
    'format.derive-schema' = 'true',
    'format.fields.0.name' = 'ctime',
    'format.fields.0.type' = 'TIMESTAMP',
    'format.fields.1.name' = 'category_id',
    'format.fields.1.type' = 'VARCHAR',
    'format.fields.2.name' = 'shop_id',
    'format.fields.2.type' = 'VARCHAR',
    'format.fields.3.name' = 'uid',
    'format.fields.3.type' = 'VARCHAR'
);

CREATE TABLE pvuv_sink(
    cdate VARCHAR,
    shop_id VARCHAR,
    shop_uv BIGINT,
    shop_pv BIGINT
) WITH (
    'connector.type' = 'filesystem',
    'connector.path' = 'file://${work_space}/countdistinct/actual.csv',
    'connector.property-version' = '1',
    'format.type' = 'csv',
    'format.property-version' = '1',
    'format.fields.0.name' = 'cdate',
    'format.fields.0.type' = 'VARCHAR',
    'format.fields.1.name' = 'shop_id',
    'format.fields.1.type' = 'VARCHAR',
    'format.fields.2.name' = 'shop_uv',
    'format.fields.2.type' = 'BIGINT',
    'format.fields.3.name' = 'shop_pv',
    'format.fields.3.type' = 'BIGINT'
);

INSERT INTO pvuv_sink
SELECT
    date_format(ctime, 'yyyy-MM-dd HH:mm:ss') as cdate, -- 将数据从时间戳格式（2018-12-04 15:44:54），转换为date格式(20181204)
    shop_id,
    count(distinct uid) as shop_uv, -- shop uv
    count(uid) as shop_pv -- show pv
FROM src
--按照天做聚合
GROUP BY date_format(ctime, 'yyyy-MM-dd HH:mm:ss'), shop_id;