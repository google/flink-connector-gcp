/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.flink.connector.gcp.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.flink.table.types.DataType;

import org.apache.flink.table.api.DataTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.table.types.DataType;

/**
 * Utility class for BigQuery types.
 */
public class BigQueryTypeUtils {

    public static DataType toFlinkType(Field bigQueryField) {
        StandardSQLTypeName typeName = bigQueryField.getType().getStandardType();
        switch (typeName) {
            case STRING:
                return DataTypes.STRING();
            case INT64:
                return DataTypes.BIGINT();
            case BOOL:
                return DataTypes.BOOLEAN();
            case FLOAT64:
                return DataTypes.DOUBLE();
            case BYTES:
                return DataTypes.BYTES();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
                return DataTypes.TIMESTAMP_LTZ();
            case TIME:
                return DataTypes.TIME();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP_LTZ();
            case NUMERIC:
                return DataTypes.DECIMAL(38, 9); // Adjust precision and scale as needed
            case BIGNUMERIC:
                return DataTypes.DECIMAL(77, 16); // Adjust precision and scale as needed
            case ARRAY:
                // TODO: Handle array mapping
                break;
            case STRUCT:
                // TODO: Handle struct mapping
                break;
            case GEOGRAPHY:
                return DataTypes.STRING(); // Or a more specific geospatial type if available
            default:
            // Consider logging or throwing an exception for unsupported types
        }
        throw new IllegalArgumentException("Unsupported BigQuery type: " + typeName);
    }

    /**
     * Defines the valid mapping between BigQuery types and native Avro types.
     *
     * <p>
     * Some BigQuery types are duplicated here since slightly different Avro
     * records are produced when exporting data in Avro format and when reading
     * data directly using the read API.
     */
    static final Map<String, List<Schema.Type>> BIG_QUERY_TO_AVRO_TYPES
            = initializeBigQueryToAvroTypesMapping();

    private static Map<String, List<Schema.Type>> initializeBigQueryToAvroTypesMapping() {
        Map<String, List<Schema.Type>> mapping = new HashMap<>();

        mapping.put("STRING", Arrays.asList(Schema.Type.STRING));
        mapping.put("GEOGRAPHY", Arrays.asList(Schema.Type.STRING));
        mapping.put("BYTES", Arrays.asList(Schema.Type.BYTES));
        mapping.put("INTEGER", Arrays.asList(Schema.Type.LONG));
        mapping.put("INT64", Arrays.asList(Schema.Type.LONG));
        mapping.put("FLOAT", Arrays.asList(Schema.Type.DOUBLE));
        mapping.put("FLOAT64", Arrays.asList(Schema.Type.DOUBLE));
        mapping.put("NUMERIC", Arrays.asList(Schema.Type.BYTES));
        mapping.put("BIGNUMERIC", Arrays.asList(Schema.Type.BYTES));
        mapping.put("BOOLEAN", Arrays.asList(Schema.Type.BOOLEAN));
        mapping.put("BOOL", Arrays.asList(Schema.Type.BOOLEAN));
        mapping.put("TIMESTAMP", Arrays.asList(Schema.Type.LONG));
        mapping.put("RECORD", Arrays.asList(Schema.Type.RECORD));
        mapping.put("STRUCT", Arrays.asList(Schema.Type.RECORD));
        mapping.put("DATE", Arrays.asList(Schema.Type.STRING, Schema.Type.INT));
        mapping.put("DATETIME", Arrays.asList(Schema.Type.STRING));
        mapping.put("TIME", Arrays.asList(Schema.Type.STRING, Schema.Type.LONG));
        mapping.put("JSON", Arrays.asList(Schema.Type.STRING));
        return mapping;
    }

    /**
     * Defines the valid mapping between BigQuery types and standard SQL types.
     */
    static final Map<String, StandardSQLTypeName> BIG_QUERY_TO_SQL_TYPES
            = initializeBigQueryToSQLTypesMapping();

    /*
      STRING, BYTES, INTEGER, INT64 (same as
     * INTEGER), FLOAT, FLOAT64 (same as FLOAT), NUMERIC, BIGNUMERIC, BOOLEAN, BOOL (same as BOOLEAN),
     * TIMESTAMP, DATE, TIME, DATETIME, INTERVAL, RECORD (where RECORD indicates that the field
     * contains a nested schema) or STRUCT (same as RECORD).
     */
    private static Map<String, StandardSQLTypeName> initializeBigQueryToSQLTypesMapping() {
        Map<String, StandardSQLTypeName> mapping = new HashMap<>();

        mapping.put("STRING", StandardSQLTypeName.STRING);
        mapping.put("BYTES", StandardSQLTypeName.BYTES);
        mapping.put("INTEGER", StandardSQLTypeName.INT64);
        mapping.put("INT64", StandardSQLTypeName.INT64);
        mapping.put("FLOAT", StandardSQLTypeName.FLOAT64);
        mapping.put("FLOAT64", StandardSQLTypeName.FLOAT64);
        mapping.put("NUMERIC", StandardSQLTypeName.NUMERIC);
        mapping.put("BIGNUMERIC", StandardSQLTypeName.BIGNUMERIC);
        mapping.put("BOOLEAN", StandardSQLTypeName.BOOL);
        mapping.put("BOOL", StandardSQLTypeName.BOOL);
        mapping.put("TIMESTAMP", StandardSQLTypeName.TIMESTAMP);
        mapping.put("DATE", StandardSQLTypeName.DATE);
        mapping.put("TIME", StandardSQLTypeName.TIME);
        mapping.put("DATETIME", StandardSQLTypeName.DATETIME);
        mapping.put("INTERVAL", StandardSQLTypeName.INTERVAL);
        mapping.put("RECORD", StandardSQLTypeName.STRUCT);
        mapping.put("STRUCT", StandardSQLTypeName.STRUCT);

        return mapping;
    }

    static List<TableFieldSchema> fieldListToListOfTableFieldSchema(FieldList fieldList) {
        return Optional.ofNullable(fieldList)
                .map(
                        fList
                        -> fList.stream()
                                .map(field -> fieldToTableFieldSchema(field))
                                .collect(Collectors.toList()))
                .orElse(new ArrayList<>());
    }

    static TableFieldSchema fieldToTableFieldSchema(Field field) {

        return new TableFieldSchema()
                .setName(field.getName())
                .setDescription(field.getDescription())
                .setDefaultValueExpression(field.getDefaultValueExpression())
                .setCollation(field.getCollation())
                .setMode(Optional.ofNullable(field.getMode()).map(m -> m.name()).orElse(null))
                .setType(field.getType().name())
                .setFields(fieldListToListOfTableFieldSchema(field.getSubFields()));
    }

    /**
     * Transforms a BigQuery {@link com.google.cloud.bigquery.Schema} into a
     * {@link TableSchema}.
     *
     * @param schema the schema from the API.
     * @return a TableSchema instance.
     */
    public static TableSchema bigQuerySchemaToTableSchema(com.google.cloud.bigquery.Schema schema) {
        return new TableSchema().setFields(fieldListToListOfTableFieldSchema(schema.getFields()));
    }

    public static StandardSQLTypeName bigQueryTableFieldSchemaTypeToSQLType(
            String tableFieldSchemaType) {
        return BIG_QUERY_TO_SQL_TYPES.getOrDefault(tableFieldSchemaType, null);
    }
}
