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

/**
 * Enum representing the supported changelog modes for the Bigtable sink.
 *
 * <ul>
 *   <li>{@link #INSERT_ONLY} -- only INSERT rows are accepted (default).
 *   <li>{@link #UPSERT} -- INSERT, UPDATE_AFTER, and DELETE rows are accepted. Requires a PRIMARY
 *       KEY.
 *   <li>{@link #ALL} -- INSERT, UPDATE_BEFORE, UPDATE_AFTER, and DELETE rows are accepted. Requires
 *       a PRIMARY KEY.
 * </ul>
 */
public enum BigtableChangelogMode {
    INSERT_ONLY("insert-only"),
    UPSERT("upsert"),
    ALL("all");

    private final String value;

    BigtableChangelogMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Parses a string value into the corresponding {@link BigtableChangelogMode}.
     *
     * @param value The string value to parse (e.g. {@code "insert-only"}, {@code "upsert"}, {@code
     *     "all"}).
     * @return The matching {@link BigtableChangelogMode}.
     * @throws IllegalArgumentException if the value does not match any supported mode.
     */
    public static BigtableChangelogMode fromString(String value) {
        for (BigtableChangelogMode mode : values()) {
            if (mode.value.equals(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                String.format(
                        "Invalid 'changelog-mode' value '%s'. "
                                + "Supported values are: 'insert-only', 'upsert', 'all'.",
                        value));
    }
}
