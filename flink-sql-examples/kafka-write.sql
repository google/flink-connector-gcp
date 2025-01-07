-- Writes data to a Managed Service for Apache Kafka topic.

-- Replace values in <ANGLE BRACKETS>
CREATE TABLE input_data (
    word STRING
 ) WITH (
   'connector' = 'datagen',
   'fields.word.length' = '5'
);

CREATE TABLE write_to_kafka (
    word STRING,
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = '<TOPIC>',
    'properties.bootstrap.servers' = 'bootstrap.<CLUSTER>.<REGION>.managedkafka.<PROJECT>.cloud.goog:9092',
    'format' = 'json',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.partition.discovery.interval.ms' = '10000',
    'properties.sasl.mechanism' = 'OAUTHBEARER',
    'properties.sasl.login.callback.handler.class' = 'com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;'
);

INSERT INTO write_to_kafka SELECT word FROM input_data;

