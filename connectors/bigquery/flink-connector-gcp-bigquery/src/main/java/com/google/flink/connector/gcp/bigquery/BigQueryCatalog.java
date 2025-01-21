package com.google.flink.connector.gcp.bigquery;

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

/**
 * Flink Catalog Support for BigQuery. This class provides the implementation
 * for Catalog operations for BigQuery.
 */
public class BigQueryCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCatalog.class);

    private Bigquery client;

    public static final String DEFAULT_DATASET = "default";

    private final String projectId;

    public BigQueryCatalog(String catalogName, String defaultDataset, String project, String credentialFile) {
        super(catalogName, defaultDataset);
        this.projectId = project;
        LOG.info("Created BigQueryCatalog: {}", catalogName);
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("Connected to BigQuery with project ID: {}", this.projectId);

        try {
            HttpCredentialsAdapter httpCredentialsAdapter
                    = new HttpCredentialsAdapter(
                            GoogleCredentials.getApplicationDefault().createScoped(BigqueryScopes.all()));

            this.client = new Bigquery.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    GsonFactory.getDefaultInstance(),
                    httpRequest -> {
                        httpCredentialsAdapter.initialize(httpRequest);
                        httpRequest.setThrowExceptionOnExecuteError(false);
                    })
                    .setApplicationName("BigQuery Flink Catalog Plugin")
                    .build();
        } catch (GeneralSecurityException | IOException e) {
            throw new CatalogException("Failed to create BigQuery client", e);
        }
    }

    @Override
    public void close() throws CatalogException {
        if (client != null && client.getRequestFactory().getTransport() instanceof com.google.api.client.http.javanet.NetHttpTransport) {
            try {
                this.client.getRequestFactory().getTransport().shutdown();
                LOG.info("Shutdown the underlying HTTP transport for BigQuery client.");
            } catch (IOException e) {
                throw new CatalogException("Failed to shutdown the HTTP transport", e);
            } finally {
                client = null;
            }
        } else {
            client = null;
        }
    }

    // -- Database Operations --
    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> targetReturnList = new ArrayList<>();
        try {
            DatasetList datasets = this.client.datasets().list(this.projectId).execute();

            if (datasets == null || datasets.getDatasets() == null) {
                LOG.debug("Project does not contain any datasets.");
                return List.of();
            }
            datasets.getDatasets().forEach(dataset
                    -> targetReturnList.add(dataset.getDatasetReference().getDatasetId()));
            return targetReturnList;
        } catch (IOException e) {
            throw new CatalogException("Failed to list databases", e);
        }

    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        try {
            Dataset dataset = this.client.datasets().get(this.projectId, databaseName).execute();
            if (dataset == null) {
                throw new DatabaseNotExistException(getName(), databaseName);
            }

            return new BigQueryCatalogDatabase(this.projectId, dataset);
        } catch (IOException e) {
            throw new CatalogException("Failed to get database " + databaseName, e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            Dataset dataset = this.client.datasets().get(this.projectId, databaseName).execute();
            return dataset != null;
        } catch (IOException e) {
            throw new CatalogException("Failed to check if database exists: " + databaseName + e);
        }
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase database,
            boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("Function createDatabase not supported yet.");
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException("Function dropDatabase not supported yet.");
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase,
            boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterDatabase not supported yet.");
    }

    // -- Table Operations --
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> targetReturnList = new ArrayList<>();
        try {
            TableList tables = this.client.tables().list(this.projectId, databaseName).execute();
            if (tables != null && tables.getTables() != null) {
                tables.getTables().forEach(table -> {
                    if (table.getType().equals("TABLE")) {
                        targetReturnList.add(table.getTableReference().getTableId());
                    }
                });
            }

            return targetReturnList;
        } catch (IOException e) {
            throw new CatalogException("Failed to list tables", e);
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> targetReturnList = new ArrayList<>();
        try {
            TableList tables = this.client.tables().list(this.projectId, databaseName).execute();

            if (tables != null && tables.getTables() != null) {
                tables.getTables().forEach(table -> {
                    if (table.getType().equals("VIEW")) {
                        targetReturnList.add(table.getTableReference().getTableId());
                    }
                });
            }

            return targetReturnList;
        } catch (IOException e) {
            throw new CatalogException("Failed to list views", e);

        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        try {
            Table table = this.client.tables().get(this.projectId, tablePath.getDatabaseName(), tablePath.getObjectName()).execute();

            if (table == null) {
                throw new TableNotExistException(getName(), tablePath);
            }

            // TableFormat will equals to Iceberg/Biglake if it's an Iceberg/Biglake table
            if (table.getBiglakeConfiguration() != null) {
                throw new CatalogException("BiglakeConfigurationis not null, external table are not supported yet.");
            }

            org.apache.flink.table.api.Schema translatedSchema
                    = BigQueryTypeUtils.toFlinkSchema(table.getSchema());

            Map<String, String> options = new HashMap<>();
            options.put("connector", "bigquery");
            options.put("project", this.projectId);
            options.put("dataset", tablePath.getDatabaseName());
            options.put("table", tablePath.getObjectName());

            CatalogTable translated_table = org.apache.flink.table.catalog.CatalogTable.of(
                    translatedSchema,
                    "",
                    Collections.emptyList(),
                    options);
            return translated_table;

        } catch (IOException e) {
            throw new CatalogException("Failed to get table " + tablePath, e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            Table table = this.client.tables().get(this.projectId, tablePath.getDatabaseName(), tablePath.getObjectName()).execute();
            return table != null;
        } catch (IOException e) {
            throw new CatalogException("Failed to check if table exists: " + tablePath, e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function dropTable not supported yet.");
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName,
            boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("Function renameTable not supported yet.");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table,
            boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function createTable not supported yet.");
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable,
            boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterTable not supported yet.");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException("Function listPartitions not supported yet.");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        throw new UnsupportedOperationException("Function listPartitions with partitionSpec not supported yet.");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException("Function listPartitionsByFilter not supported yet.");
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function getPartition not supported yet.");
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException("Function partitionExists not supported yet.");
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException("Function createPartition not supported yet.");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function dropPartition not supported yet.");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterPartition not supported yet.");
    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function listFunctions not supported yet.");
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function getFunction not supported yet.");
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        throw new UnsupportedOperationException("Function functionExists not supported yet.");
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function,
            boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function createFunction not supported yet.");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction,
            boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterFunction not supported yet.");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function dropFunction not supported yet.");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        try {
            Table table = this.client.tables().get(this.projectId, tablePath.getDatabaseName(), tablePath.getObjectName()).execute();

            if (table != null) {
                BigInteger numRows = table.getNumRows();

                int fileCount = -1;
                long totalSize = -1;
                long rawDataSize = -1;

                java.util.Map<String, String> properties = new java.util.HashMap<>();
                if (table.getDescription() != null) {
                    properties.put("description", table.getDescription());
                }
                if (table.getCreationTime() != null) {
                    properties.put("creationTime", String.valueOf(table.getCreationTime()));
                }
                if (table.getLastModifiedTime() != null) {
                    properties.put("lastModifiedTime", String.valueOf(table.getLastModifiedTime()));
                }
                return new CatalogTableStatistics(numRows != null ? numRows.longValue() : 0,
                        fileCount,
                        totalSize,
                        rawDataSize,
                        properties
                );

            } else {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (IOException e) {
            if (e instanceof GoogleJsonResponseException && ((GoogleJsonResponseException) e).getStatusCode() == 404) {
                throw new TableNotExistException(getName(), tablePath);
            }
            throw new CatalogException("Failed to get table statistics for " + tablePath, e);
        }
    }

    // Currently placeholder implementation, in order to get SELECT * FROM table to work.
    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData = new HashMap<>();
        Map<String, String> properties = new HashMap<>();
        return new CatalogColumnStatistics(
                columnStatisticsData,
                properties);
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function getPartitionStatistics not supported yet.");
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function getPartitionColumnStatistics not supported yet.");
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterTableStatistics not supported yet.");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException("Function alterTableColumnStatistics not supported yet.");
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterPartitionStatistics not supported yet.");
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterPartitionColumnStatistics not supported yet.");
    }
}
