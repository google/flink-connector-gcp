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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * Schema conversion between BigQuery and Flink.
 */
public class BigQueryTypeUtils {

    public static org.apache.flink.table.api.Schema toFlinkSchema(TableSchema bigQuerySchema) {
        List<TableFieldSchema> bigQueryFields = bigQuerySchema.getFields();
        org.apache.flink.table.api.Schema.Builder schemaBuilder
                = org.apache.flink.table.api.Schema.newBuilder();
        for (TableFieldSchema bigQueryField : bigQueryFields) {
            schemaBuilder.column(bigQueryField.getName(), toFlinkType(bigQueryField));
        }
        return schemaBuilder.build();
    }

    public static DataType toFlinkType(TableFieldSchema bigQueryField) {
        String typeName = bigQueryField.getType();
        switch (typeName) {
            case "STRING":
                return DataTypes.STRING();
            case "INTEGER":
                return DataTypes.BIGINT();
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "FLOAT":
                return DataTypes.DOUBLE();
            case "BYTES":
                return DataTypes.BYTES();
            case "DATE":
                return DataTypes.DATE();
            case "DATETIME":
                return DataTypes.TIMESTAMP_LTZ();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP_LTZ();
            case "NUMERIC":
                return DataTypes.DECIMAL(38, 9);
            case "BIGNUMERIC":
                return DataTypes.BYTES();
            case "RANGE":
                TableFieldSchema.RangeElementType elementType = bigQueryField.getRangeElementType();                
                String subTypeName = elementType.getType();
                TableFieldSchema elementField = new TableFieldSchema().setType(subTypeName);
                DataType elementFlinkType = toFlinkType(elementField);
                return DataTypes.ROW(
                        DataTypes.FIELD("lower", elementFlinkType),
                        DataTypes.FIELD("upper", elementFlinkType)
                );
            case "RECORD":
                List<TableFieldSchema> subFields = bigQueryField.getFields();
                List<DataTypes.Field> flinkFields = new ArrayList<>();
                for (TableFieldSchema subField : subFields) {
                    flinkFields.add(DataTypes.FIELD(subField.getName(), toFlinkType(subField)));
                }
                return DataTypes.ROW(flinkFields);
            case "GEOGRAPHY":
                return DataTypes.STRING();
            case "JSON":
                return DataTypes.STRING();
            default:
        }
        throw new IllegalArgumentException("Unsupported BigQuery type: " + typeName);
    }
}