# Flink Examples

This repository contains GCP connector examples

## GMK

This is assuming GMK is already set up. If not, learn more [here](https://cloud.google.com/products/apache-kafka-for-bigquery)

To quickly try a GMK example, go to `gmk-to-gmk-oauth.yaml` and replace the variables in < > with your actual values.

`kubectl apply -f gmk-to-gmk-oauth.yaml`

### Authentication

Authenticating with Oauth for GMK just requires adding a few properties when building the GMK source/sink.

```
.setProperty("security.protocol", "SASL_SSL")
.setProperty("sasl.mechanism", "OAUTHBEARER")
.setProperty("sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
.setProperty(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
```
