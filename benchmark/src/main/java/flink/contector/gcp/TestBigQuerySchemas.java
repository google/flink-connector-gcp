/*
 * Copyright (C) 2024 Google Inc.
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
package flink.contector.gcp;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.Arrays;
import java.util.List;

public class TestBigQuerySchemas {

    // Private Constructor to ensure no instantiation.
    private TestBigQuerySchemas() {}

    public static BigQuerySchemaProvider getSchemaWithStringTypes(String mode) {
        List<TableFieldSchema> fields =
                Arrays.asList(
                    new TableFieldSchema().setName("word").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("countStr").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("word1").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("countStr1").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("word2").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("countStr2").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("word3").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("countStr3").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("word4").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("countStr4").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("word5").setType("STRING").setMode(mode),
                    new TableFieldSchema().setName("countStr5").setType("STRING").setMode(mode)
                );
        TableSchema tableSchema = new TableSchema().setFields(fields);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProviderImpl(tableSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new TestSchemaProvider(bigQuerySchemaProvider.getAvroSchema(), descriptor);
    }

    public static BigQuerySchemaProvider getSchemaWithPrimitiveTypes(String mode) {
        List<TableFieldSchema> fields =
            Arrays.asList(
                new TableFieldSchema().setName("number").setType("INTEGER").setMode(mode),
                new TableFieldSchema().setName("price").setType("FLOAT").setMode(mode),
                new TableFieldSchema().setName("species").setType("STRING").setMode(mode),
                new TableFieldSchema().setName("flighted").setType("BOOLEAN").setMode(mode),
                new TableFieldSchema().setName("number1").setType("INTEGER").setMode(mode),
                new TableFieldSchema().setName("price1").setType("FLOAT").setMode(mode),
                new TableFieldSchema().setName("species1").setType("STRING").setMode(mode),
                new TableFieldSchema().setName("flighted1").setType("BOOLEAN").setMode(mode),
                new TableFieldSchema().setName("number2").setType("STRING").setMode(mode),
                new TableFieldSchema().setName("price2").setType("STRING").setMode(mode));
        TableSchema tableSchema = new TableSchema().setFields(fields);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProviderImpl(tableSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new TestSchemaProvider(bigQuerySchemaProvider.getAvroSchema(), descriptor);
    }

    public static BigQuerySchemaProvider getSchemaWithRequiredPrimitiveTypes() {
        return getSchemaWithPrimitiveTypes("REQUIRED");
    }

    public static BigQuerySchemaProvider getSchemaWithNullablePrimitiveTypes() {
        return getSchemaWithPrimitiveTypes("NULLABLE");
    }

}