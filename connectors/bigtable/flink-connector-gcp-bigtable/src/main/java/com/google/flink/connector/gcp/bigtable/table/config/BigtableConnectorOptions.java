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

package com.google.flink.connector.gcp.bigtable.table.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configurations for a Bigtable Table API Write. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
public class BigtableConnectorOptions {

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP project for Bigtable.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the Bigtable table name.");

    public static final ConfigOption<String> INSTANCE =
            ConfigOptions.key("instance")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the Bigtable instance name.");

    public static final ConfigOption<String> COLUMN_FAMILY =
            ConfigOptions.key("column-family")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the Bigtable column family name.");

    public static final ConfigOption<Boolean> USE_NESTED_ROWS_MODE =
            ConfigOptions.key("use-nested-rows")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Specifies the use of nested rows are column families.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Sink Parallelism");
}
