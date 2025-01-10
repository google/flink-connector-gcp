package com.google.flink.connector.gcp.bigquery.client;

import java.io.IOException;
import java.security.GeneralSecurityException;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

/**
 * Implementation of the BigQuery client for the Managed Flink BigQuery Catalog.
 */
public class BigQueryClient {

    public final BigQuery client;

    /**
     * Constructor for the BigQuery client.
     *
     * @throws IOException if the client cannot be created
     * @throws GeneralSecurityException if the client cannot be created
     */
    public BigQueryClient() throws IOException, GeneralSecurityException {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        this.client = bigquery;
    }

}
