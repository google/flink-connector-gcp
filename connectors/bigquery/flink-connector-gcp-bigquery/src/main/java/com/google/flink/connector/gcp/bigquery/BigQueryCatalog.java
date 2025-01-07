package com.google.flink.connector.gcp.bigquery;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
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
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.flink.connector.gcp.bigquery.client.BigQueryClient;

/** Catalog for BigQuery. */
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
        LOG.info("Connected to BigQuery metastore with project ID: {}", this.projectId);
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("BigQuery Catalog support Closed.");
    }


    // -- Database Operations --

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> targetReturnList = new ArrayList<>();
        try {
            Page<Dataset> datasets = bigqueryclient.client.listDatasets(this.projectId, DatasetListOption.pageSize(100));
            if (datasets == null) {
                System.out.println("Dataset does not contain any models");
                return List.of();
            }
            datasets
                  .iterateAll()
                  .forEach(
                      dataset -> targetReturnList.add(String.format("Success! Dataset ID: %s ", dataset.getDatasetId())));
            return targetReturnList;
        } catch (Exception e) {
            throw new CatalogException("Failed to list databases", e);
        }
        
    }
    
    // get functions not supported yet.
    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function getDatabase not supported yet.");

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

    // ignoreIfExists Not supported yet.
    @Override
    public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        try {
            if (!ignoreIfExists & databaseExists(databaseName)) {
                throw new DatabaseAlreadyExistException(getName(), databaseName);
            }
            DatasetInfo datasetInfo = DatasetInfo.newBuilder(databaseName).build();
            Dataset newDataset = bigqueryclient.client.create(datasetInfo);
            if (database.getProperties().containsKey("description")) {
                String description = database.getProperties().get("description");
                bigqueryclient.client.update(newDataset.toBuilder().setDescription(description).build());
            }
            String newDatasetName = newDataset.getDatasetId().getDataset();
            LOG.info(newDatasetName + " created successfully;");
        } catch (DatabaseAlreadyExistException e) {
            throw e;
        } catch (BigQueryException e) {
            throw new CatalogException("Failed to create database " + databaseName, e);
        }
    }

    // Flag cascade not tested yet.
    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        try {
            if (!databaseExists(databaseName)) {
                if (ignoreIfNotExists) {
                        LOG.info("Trying to drop non-existent database {} but ignore flag is set.", databaseName);
                        return;
                    } else {
                        throw new DatabaseNotExistException(getName(), databaseName);
                    }
                }
                // Drop all tables in the database if cascade is set.
                if (cascade) {
                    List<String> tables = listTables(databaseName);
                    if (!tables.isEmpty()) {
                        LOG.info("Dropping all tables in database {} due to cascade option.", databaseName);
                        for (String tableName : tables) {
                            try {
                                dropTable(new ObjectPath(databaseName, tableName), false);
                            } catch (TableNotExistException e) {
                                // Should not happen given listTables, but handle defensively
                                LOG.warn("Table {} not found during cascade drop of database {}.", tableName, databaseName, e);
                            }
                        }
                    }
                } else {
                    // Throw an exception based on the cascade flag
                    List<String> tables = listTables(databaseName);
                    if (!tables.isEmpty()) {
                        throw new DatabaseNotEmptyException(getName(), databaseName);
                    }
                }
                
                DatasetId datasetId = DatasetId.of(this.projectId, databaseName);
                boolean deleted = bigqueryclient.client.delete(datasetId);
                if (!deleted && !ignoreIfNotExists) {
                    // This case is unlikely if databaseExists check passed, but for robustness
                    throw new DatabaseNotExistException(getName(), databaseName);
                }
    
            } catch (DatabaseNotExistException | DatabaseNotEmptyException e) {
                throw e;
            } catch (CatalogException e) {
                throw new CatalogException("Failed to drop database " + databaseName, e);
            }
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
            tables.iterateAll().forEach(table -> targetReturnList.add(String.format("Success! Table ID: %s ", table.getTableId().getTable())));
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

    // Get functions not supported yet.
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        try {
            DatasetId datasetId = DatasetId.of(this.projectId, tablePath.getDatabaseName());
            TableId tableId = TableId.of(datasetId.getDataset(), tablePath.getObjectName());
            Table table = bigqueryclient.client.getTable(tableId);

            // TODO: Need to map BigQuery schema to Flink schema
            // Also, partitioning and clustering keys are not supported yet.
            throw new UnsupportedOperationException("Function getTable not supported yet.");
        } catch (BigQueryException e) {
            throw new CatalogException("Failed to get table " + tablePath, e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            DatasetId datasetId = DatasetId.of(this.projectId, tablePath.getDatabaseName());
            TableId tableId = TableId.of(datasetId.getDataset(), tablePath.getObjectName());
            Table table = bigqueryclient.client.getTable(tableId);
            return table != null;
        } catch (BigQueryException e) {
            if (e.getCode() == 404) { // NOT_FOUND
                return false;
            }
            throw new CatalogException("Failed to check if table exists: " + tablePath, e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        try {
            DatasetId datasetId = DatasetId.of(this.projectId, tablePath.getDatabaseName());
            TableId tableId = TableId.of(datasetId.getDataset(), tablePath.getObjectName());

            if (!tableExists(tablePath)) {
                if (ignoreIfNotExists) {
                    LOG.info("Trying to drop non-existent table {} but ignore flag is set.", tablePath);
                    return;
                } else {
                    throw new TableNotExistException(getName(), tablePath);
                }
            }

            boolean deleted = bigqueryclient.client.delete(tableId);
            if (!deleted && !ignoreIfNotExists) {
                // This case is unlikely if tableExists check passed, but for robustness
                throw new TableNotExistException(getName(), tablePath);
            }

        } catch (TableNotExistException e) {
            throw e;
        } catch (BigQueryException e) {
            throw new CatalogException("Failed to drop table " + tablePath, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        try {
            DatasetId datasetId = DatasetId.of(this.projectId, tablePath.getDatabaseName());
            TableId oldTableId = TableId.of(datasetId.getDataset(), tablePath.getObjectName());
            TableId newTableId = TableId.of(datasetId.getDataset(), newTableName);

            // Handling ignoreIfNotExists flag
            if (!tableExists(tablePath)) {
                if (ignoreIfNotExists) {
                    LOG.info("Trying to rename non-existent table {} but ignore flag is set.", tablePath);
                    return;
                } else {
                    throw new TableNotExistException(getName(), tablePath);
                }
            }

            // Check if the new table name already exists in the dataset
            ObjectPath newTablePath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
            if (tableExists(newTablePath)) {
                throw new TableAlreadyExistException(getName(), newTablePath);
            }

            Table currentTable = bigqueryclient.client.getTable(oldTableId);
            if (currentTable != null) {
                Table renamedTable = currentTable.toBuilder().setTableId(newTableId).build();
                boolean success = bigqueryclient.client.update(renamedTable) != null;
                if (!success) {
                    throw new CatalogException("Failed to rename table " + tablePath + " to " + newTableName);
                }
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }

        } catch (TableNotExistException | TableAlreadyExistException e) {
            throw e;
        } catch (BigQueryException e) {
            throw new CatalogException("Failed to rename table " + tablePath + " to " + newTableName, e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        try {   
            DatasetId datasetId = DatasetId.of(this.projectId, tablePath.getDatabaseName());
            TableId tableId = TableId.of(datasetId.getDataset(), tablePath.getObjectName());
            
            // Handling ignoreIfNotExists flag
            if (tableExists(tablePath)) {
                if (ignoreIfExists) {
                    LOG.info("Table {} already exists but ignore flag is set.", tablePath);
                    return;
                } else {
                    throw new TableAlreadyExistException(getName(), tablePath);
                }
            }

            // TODO: Implement support for CatalogBaseTable properties.
            Schema default_schema =
                Schema.of(
                    Field.of("stringField", StandardSQLTypeName.STRING),
                    Field.of("booleanField", StandardSQLTypeName.BOOL));
            TableDefinition tableDefinition = StandardTableDefinition.of(default_schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            bigqueryclient.client.create(tableInfo);
            LOG.info("Table {} created successfully.", tablePath);
        } catch (BigQueryException e) {
            LOG.info("Table was not created. \n" + e.toString());
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function alterTable not supported yet.");
    }


    // -- Partition Operations --
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

    // -- Function Operations --

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
        throw new UnsupportedOperationException("Function getTableStatistics not supported yet.");
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Function getTableColumnStatistics not supported yet.");
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