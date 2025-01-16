package com.google.flink.connector.gcp.bigquery;

import java.io.IOException;
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

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.flink.connector.gcp.bigquery.client.BigQueryClient;

/**
 * Flink Catalog Support for BigQuery. This class provides the implementation for
 * Catalog operations for BigQuery.
 */
public class BigQueryCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCatalog.class);

    private final BigQueryClient bigqueryclient;

    public static final String DEFAULT_DATASET = "default";

    private final String projectId;

    public BigQueryCatalog(String catalogName, String defaultDataset, String project, String credentialFile) throws IOException, GeneralSecurityException {
        super(catalogName, defaultDataset);
        this.projectId = project;

        try {
            this.bigqueryclient = new BigQueryClient();
        } catch (CatalogException e) {
            throw new CatalogException("Failed to create BigQuery client", e);
        }
        LOG.info("Created BigQueryCatalog: {}", catalogName);
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("Connected to BigQuery with project ID: {}", this.projectId);
    }

    @Override
    public void close() throws CatalogException {
    }

    // -- Database Operations --
    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> targetReturnList = new ArrayList<>();
        try {
            Page<Dataset> datasets = bigqueryclient.client.listDatasets(this.projectId, DatasetListOption.pageSize(100));
            if (datasets == null) {
                LOG.info("Project does not contain any datasets.");
                return List.of();
            }
            datasets
                    .iterateAll()
                    .forEach(
                            dataset -> targetReturnList.add(String.format("%s", dataset.getDatasetId().getDataset())));
            return targetReturnList;
        } catch (Exception e) {
            throw new CatalogException("Failed to list databases", e);
        }

    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        DatasetId datasetId = DatasetId.of(this.projectId, databaseName);
        Dataset dataset = bigqueryclient.client.getDataset(datasetId);
        if (dataset == null) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return new BigQueryCatalogDatabase(this.projectId, dataset);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            Page<Dataset> datasets = bigqueryclient.client.listDatasets(this.projectId);
            if (datasets != null) {
                for (Dataset dataset : datasets.iterateAll()) {
                    if (dataset.getDatasetId().getDataset().equals(databaseName)) {
                        return true;
                    }
                }
            }
            return false;
        } catch (BigQueryException e) {
            throw new CatalogException("Failed to check if database exists: " + databaseName, e);
        }
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("Function createDatabase not supported yet.");
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException("Function dropDatabase not supported yet.");
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterDatabase not supported yet.");
    }

    // -- Table Operations --
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> targetReturnList = new ArrayList<>();
        try {
            DatasetId datasetId = DatasetId.of(this.projectId, databaseName);
            Page<Table> tables = bigqueryclient.client.listTables(datasetId, TableListOption.pageSize(100));

            tables.iterateAll().forEach(table -> targetReturnList.add(String.format("%s, (Type: %s)",
                    table.getTableId().getTable(), table.getDefinition().getType().name())));

            return targetReturnList;

        } catch (BigQueryException e) {
            return List.of();
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> targetReturnList = new ArrayList<>();
        try {
            DatasetId datasetId = DatasetId.of(this.projectId, databaseName);
            Page<Table> tables = bigqueryclient.client.listTables(datasetId, TableListOption.pageSize(100));

            if (tables != null) {
                tables.iterateAll().forEach(table -> {
                    if (table.getDefinition().getType() == TableDefinition.Type.VIEW) {
                        targetReturnList.add(String.format("Success! View ID: %s ", table.getTableId().getTable()));
                    }
                });
            }

            return targetReturnList;
        } catch (BigQueryException e) {
            return List.of();
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        try {
            DatasetId datasetId = DatasetId.of(this.projectId, tablePath.getDatabaseName());
            TableId tableId = TableId.of(datasetId.getDataset(), tablePath.getObjectName());
            Table table = bigqueryclient.client.getTable(tableId);

            if (table == null) {
                throw new TableNotExistException(getName(), tablePath);
            }

            org.apache.flink.table.api.Schema translatedSchema
                    = BigQueryTypeUtils.toFlinkSchema(table.getDefinition().getSchema());

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

        } catch (BigQueryException e) {
            throw new CatalogException("Failed to get table " + tablePath, e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        throw new UnsupportedOperationException("Function tableExists not supported yet.");
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function dropTable not supported yet.");
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("Function renameTable not supported yet.");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function createTable not supported yet.");
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
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
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException("Function createPartition not supported yet.");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function dropPartition not supported yet.");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
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
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function createFunction not supported yet.");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterFunction not supported yet.");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function dropFunction not supported yet.");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        try {
            DatasetId datasetId = DatasetId.of(this.projectId, tablePath.getDatabaseName());
            TableId tableId = TableId.of(datasetId.getDataset(), tablePath.getObjectName());
            Table table = bigqueryclient.client.getTable(tableId);

            if (table != null) {
                Long numRows = table.getNumRows() != null ? table.getNumRows().longValue() : null;

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
                return new CatalogTableStatistics(numRows != null ? numRows : 0,
                        fileCount,
                        totalSize,
                        rawDataSize,
                        properties
                );

            } else {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (BigQueryException e) {
            if (e.getCode() == 404) {
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
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterTableStatistics not supported yet.");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException("Function alterTableColumnStatistics not supported yet.");
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterPartitionStatistics not supported yet.");
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterPartitionColumnStatistics not supported yet.");
    }
}
