# BigQuery Catalog developing notes

## Build command

mvn clean package -DskipTests


## Test command -> the Pipeline

## Launch SQL client with JAR path:
./bin/sql-client.sh 

### ADD JAR
ADD JAR '/Users/boqianshi/workspace/flink-connector-gcp/connectors/bigquery/flink-connector-gcp-bigquery/target/flink-connector-gcp-bigquery-0.1.0-SNAPSHOT.jar';

### SQL run catalog command
CREATE CATALOG bq_test WITH ('type' = 'bigquery','bigquery-project' = 'gmf-eng-internal-06a','credential-file' = '~/.config/gcloud/application_default_credentials.json', 'default-dataset' = 'boqian1');

### Table create test command:
CREATE TABLE createtable_test (
    column1 INT,
    column2 STRING,
    column3 BOOLEAN
)
WITH (
    'connector' = 'bigquery',
    'table' = 'createtable_test'
);


### Schema test
CREATE TABLE create_test WITH (
    'connector' = 'bigquery',
    'project' = 'gmf-eng-internal-06a',
    'dataset' = 'boqian1',
    'table' = 'country_codes'
);

SELECT * FROM create_test LIMIT 10;

### Questions to ask:
1. getTable/getDatabase returns a CatalogDatabase/CatalogTable, which is a Flink object, while all other functions are modifying things within BigQuery. Schema mapping? What should be included in the property?

```
public CatalogDatabaseImpl(Map<String, String> properties, @Nullable String comment) {
        this.properties = checkNotNull(properties, "properties cannot be null");
        this.comment = comment;
    }
```

2. Tests related.

3. Maven related -> Merge with BigTable?

4. Partitions: Time/Range Paritition in BigQuery

5. Functions: Didn't find in BQ



#### Connector 
CREATE TABLE test_bq_connector (
    country_name STRING, 
    alpha_2_code STRING, 
    alpha_3_code STRING
) WITH (
    'connector' = 'bigquery', 
    'project' = 'gmf-eng-internal-06a', 
    'dataset' = 'boqian1',
    'table' = 'country_codes'
);