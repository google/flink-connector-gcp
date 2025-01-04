package com.google.flink.connector.gcp.bigquery.client;

import java.io.IOException;
import java.security.GeneralSecurityException;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
// Execution command:
// mvn exec:java -Dexec.mainClass="com.google.flink.connector.gcp.bigquery.client.BigQueryClient"

/** Implementation of the BigQuery client for the Managed Flink BigQuery Catalog. */
public class BigQueryClient {

  public final BigQuery client;

  public BigQueryClient() throws IOException, GeneralSecurityException {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    this.client = bigquery;
  }

}
