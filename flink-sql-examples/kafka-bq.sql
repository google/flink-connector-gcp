-- Reads from Managed Service for Apache Kafka and writes to BigQuery.

-- Replace values in <ANGLE BRACKETS>
CREATE TABLE kafka_source (
    word STRING,
    proctime AS PROCTIME() -- Generate processing-time attribute using computed column.
) WITH (
    'connector' = 'kafka',
    'topic' = '<TOPIC>',
    'scan.startup.mode' = 'earliest-offset',  -- Read from the beginning.
    'properties.bootstrap.servers' = 'bootstrap.<CLUSTER>.<REGION>.managedkafka.<PROJECT>.cloud.goog:9092',
    'format' = 'json',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.partition.discovery.interval.ms' = '10000',
    'properties.sasl.mechanism' = 'OAUTHBEARER',
    'properties.sasl.login.callback.handler.class' = 'com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;'
);


CREATE TABLE destination (
    word STRING,
    countStr STRING
) WITH (
    'connector' = 'bigquery',
    'project' = '<PROJECT>',
    'dataset' = '<DATASET>',
    'table' = '<TABLE>'
);


-- Query
INSERT INTO destination
SELECT word, CAST(COUNT(*) AS STRING) AS countStr
FROM TABLE(TUMBLE(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '30' SECONDS))
GROUP BY word, window_start, window_end;

