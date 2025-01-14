# Using HBase with GCP
With BigQuery Engine For Apache Flink, you can connect to HBase tables running in GCP or outside of GCP. You can also use the HBase connector to interact with GCP Bigtable tables if you prefer that interface over the[native Bigtable connector](https://github.com/google/flink-connector-gcp/tree/73493f55bc527cf7e9d1b2bf9b405ec088cdb6eb/connectors/bigtable). 

## Write to an HBase Table 
Create a Managed flink deployment:

```
gcloud alpha managed-flink deployments create my-deployment \
  --project=$PROJECT_ID \
  --location=us-central1 \
  --network-config-vpc=$NETWORK_NAME \
  --network-config-subnetwork=$SUBNET_NAME \
  --max-slots=4
```

Ensure that the subnet hosting your Managed Flink deployment has access to the network hosting your HBase table.

Use the [hbase.sql](./hbase.sql) example SQL job. Make sure to replace the `zookeeper.quorum` parameter with the IP address of your HBase table.

From inside of `flink-examples-gcp/sql/hbase/` folder, build the required dependencies:

```
mvn clean install -U -DskipTests -Dcheckstyle.skip
```

This will output a jar in `./target/flink-sql-example-hbase-0.0.0.jar`. This jar contains all of the required dependencies for connecting to HBase.

You can now use this jar and deployment to create a job:

```
gcloud alpha managed-flink jobs create ./hbase.sql \
  --name=my-job \
  --location=us-central1 \
  --deployment=my-deployment \
  --project=$PROJECT_ID \
  --staging-location=gs://$BUCKET_NAME/jobs/ \
  --min-parallelism=1 \
  --max-parallelism=4 \
  --jars=./target/flink-sql-example-hbase-0.0.0.jar
  ```

## Write to Bigtable via the HBase Connector

To connect to Bigtable using the HBase connector, first create a Managed flink deployment:

```
gcloud alpha managed-flink deployments create my-deployment \
  --project=$PROJECT_ID \
  --location=us-central1 \
  --network-config-vpc=$NETWORK_NAME \
  --network-config-subnetwork=$SUBNET_NAME \
  --max-slots=4
```

Ensure that the subnet hosting your Managed Flink deployment has access to the network hosting your Bigtable table.

Grant the service account running your Managed flink deployment access to Bigtable:

```
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:gmf-$PROJECT_NUMBER-default@gcp-sa-managedflink-wi.iam.gserviceaccount.com" --role=roles/bigtable.user
```

Use the [hbaseBT.sql](./hbaseBT.sql) example SQL job. Make sure to replace the values in `<brackets>` with your Bigtable configuration.

From inside of `flink-examples-gcp/sql/hbase/` folder, build the required dependencies:

```
mvn clean install -U -DskipTests -Dcheckstyle.skip
```

This will output a jar in `./target/flink-sql-example-hbase-0.0.0.jar`. This jar contains all of the required dependencies for connecting to Bigtable using HBase.

You can now use this jar and deployment to create a job:

```
gcloud alpha managed-flink jobs create ./hbaseBT.sql \
  --name=my-job \
  --location=us-central1 \
  --deployment=my-deployment \
  --project=$PROJECT_ID \
  --staging-location=gs://$BUCKET_NAME/jobs/ \
  --min-parallelism=1 \
  --max-parallelism=4 \
  --jars=./target/flink-sql-example-hbase-0.0.0.jar
  ```
