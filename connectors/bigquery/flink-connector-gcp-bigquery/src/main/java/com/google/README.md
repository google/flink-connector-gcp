# BigQuery developing notes

## Build command

mvn clean package -DskipTests


## Test command

### ADD JAR
ADD JAR '/Users/boqianshi/workspace/flink-connector-gcp/connectors/bigquery/flink-connector-gcp-bigquery/target/flink-connector-gcp-bigquery-0.1.0-SNAPSHOT.jar';

### SQL run catalog command
CREATE CATALOG bq12302024 WITH ('type' = 'bigquery','bigquery-project' = 'gmf-eng-internal-06a','credential-file' = '~/.config/gcloud/application_default_credentials.json', 'default-dataset' = 'boqian1');