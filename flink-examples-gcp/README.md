# Flink Examples

This repository contains GCP connector examples

## GMK

This is assuming Apache Kafka for BigQuery is already set up. If not, learn more [here](https://cloud.google.com/products/apache-kafka-for-bigquery)

### Authentication

Authenticating with Oauth for Apache Kafka for BigQuery just requires adding a few properties when building the Apache Kafka for BigQuery source/sink.

```
.setProperty("security.protocol", "SASL_SSL")
.setProperty("sasl.mechanism", "OAUTHBEARER")
.setProperty("sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
.setProperty(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
```

For oauth to work in a custom jar, make sure to include the GMK auth dependency.

```
<dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>google-managed-kafka-auth-login-handler</artifactId>
        <version>1.0.0</version>
</dependency>
```
