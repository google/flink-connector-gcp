/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.flink.connector.gcp.bigtable.utils;

/** Collection of errors. */
public class ErrorMessages {

    public static final String COLUMN_FAMILY_AND_NESTED_INCOMPATIBLE =
            "withNestedRowsMode and withColumnFamily are incompatible";
    public static final String COLUMN_FAMILY_OR_NESTED_ROWS_REQUIRED =
            "Either withNestedRowsMode and withColumnFamily needs to be set";
    public static final String UNSUPPORTED_SERIALIZATION_TYPE =
            "Unsupported type, use Bytes for more complex types: ";
    public static final String BASE_NO_NESTED_TYPE =
            "Nested Rows mode require all non-key fields to be of type ";
    public static final String NESTED_TYPE_ERROR =
            "Nested rows are only supported with withNestedRowsMode and not double nested. Use Bytes for more complex types";
    public static final String ROW_KEY_STRING_TYPE_TEMPLATE =
            "Row Key has to be of type String, got %s.";
    public static final String SERIALIZER_ERROR =
            "Error while serializing element to RowMutationEntry: ";
    public static final String METRICS_ENTRY_SERIALIZATION_WARNING =
            "Error while serializing RowMutationEntry for metrics, entry will be counted as 0 bytes. This error doesn't affect the job. Error: ";
    public static final String TIMESTAMP_OUTSIDE_PRECISION_TEMPLATE =
            "Unsupported precision, needs to be between %s and %s, got %s";
    public static final String MISSING_ROW_KEY_TEMPLATE = "Row key field %s not found in schema %s";
    public static final String ROW_KEY_FIELD_NULL = "Row key field must be set";
    public static final String SCHEMA_NULL = "Row key field must be set";
    public static final String MULTIPLE_PRIMARY_KEYS =
            "There must be exactly one primary key, found %d.";
}
