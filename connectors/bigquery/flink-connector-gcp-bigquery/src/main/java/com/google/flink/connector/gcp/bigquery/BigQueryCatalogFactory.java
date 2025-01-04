package com.google.flink.connector.gcp.bigquery;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.flink.connector.gcp.bigquery.BigQueryCatalogFactoryOptions.BIGQUERY_PROJECT;
import static com.google.flink.connector.gcp.bigquery.BigQueryCatalogFactoryOptions.CREDENTIAL_FILE;
import static com.google.flink.connector.gcp.bigquery.BigQueryCatalogFactoryOptions.DEFAULT_DATASET;

/** Catalog factory for BigQuery Catalog. */
public class BigQueryCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return "bigquery";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BIGQUERY_PROJECT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATASET);
        options.add(CREDENTIAL_FILE);
        options.add(BIGQUERY_PROJECT); // Add this
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        LOG.info("BigQueryCatalogFactory.createCatalog() called with context: {}", context.getName());
        System.out.printf("BigQueryCatalogFactory.createCatalog() called with context: %s%n", context.getName());
        
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        LOG.info("Configuration options validated.");

        try {
            String defaultDataset = helper.getOptions().get(DEFAULT_DATASET);
            String bigqueryProject = helper.getOptions().get(BIGQUERY_PROJECT);
            String credentialFile = helper.getOptions().get(CREDENTIAL_FILE);
            LOG.info("Creating BigQueryCatalog with defaultDataset: {}, bigqueryProject: {}, credentialFile: {}", defaultDataset, bigqueryProject, credentialFile);
            BigQueryCatalog catalog = new BigQueryCatalog(
                    context.getName(),
                    defaultDataset,
                    bigqueryProject,
                    credentialFile
            );
            LOG.info("BigQueryCatalog created successfully.");
            return catalog;
        } catch (IOException | GeneralSecurityException ex) {
            LOG.error("Error creating BigQueryCatalog: {}", ex.getMessage(), ex);
            return null;
        } catch (RuntimeException ex) { // Catch any other potential runtime exceptions
            LOG.error("Unexpected runtime error during BigQueryCatalog creation: {}", ex.getMessage(), ex);
            return null;
        }
    }
}