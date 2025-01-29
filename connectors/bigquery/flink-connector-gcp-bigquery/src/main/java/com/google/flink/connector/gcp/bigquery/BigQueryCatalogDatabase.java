package com.google.flink.connector.gcp.bigquery;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.flink.table.catalog.CatalogDatabase;

import com.google.api.services.bigquery.model.Dataset;

/**
 * A lightweight CatalogDatabase implementation for BigQuery, primarily used for
 * mapping purposes. It holds the essential information (projectId and
 * datasetId) to identify a BigQuery dataset.
 */
public class BigQueryCatalogDatabase implements CatalogDatabase {

    private final String projectId;
    private final Dataset dataset;

    /**
     * Creates a BigQueryCatalogDatabase instance.
     *
     * @param projectId the project ID of the dataset
     * @param dataset the dataset
     */
    public BigQueryCatalogDatabase(String projectId, Dataset dataset) {
        this.projectId = projectId;
        this.dataset = dataset;
    }

    public String getProjectId() {
        return projectId;
    }

    public Dataset getDatasetId() {
        return dataset;
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> property = Collections.emptyMap();
        property.put("projectId", projectId);
        property.put("datasetId", dataset.getId());
        property.put("description", dataset.getDescription());
        property.put("location", dataset.getLocation());
        return property;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigQueryCatalogDatabase that = (BigQueryCatalogDatabase) o;
        return Objects.equals(projectId, that.projectId) && Objects.equals(dataset, that.dataset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectId, dataset);
    }

    @Override
    public String toString() {
        return "BigQueryCatalogDatabase{"
                + "projectId='" + projectId + '\''
                + ", datasetId='" + dataset.getId() + '\''
                + '}';
    }

    @Override
    public String getComment() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CatalogDatabase copy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CatalogDatabase copy(Map<String, String> properties) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.ofNullable(dataset.getDescription());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.ofNullable(dataset.getDescription());
    }

}
