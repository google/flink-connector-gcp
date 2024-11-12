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

package com.google.flink.connector.gcp.bigtable.testingutils;

import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A utility class providing constants and helper methods for testing Bigtable related
 * functionalities.
 *
 * <p>This class contains commonly used constant values for table, instance, and project identifiers
 * in tests, as well as a method to generate a sample {@link RowMutationEntry} and a method to
 * assert equality between two {@link RowMutationEntry} objects.
 */
public class TestingUtils {
    public static final String TABLE = "table";
    public static final String INSTANCE = "instance";
    public static final String PROJECT = "project";
    public static final String COLUMN_FAMILY = "testColumnFamily";
    public static final String ROW_KEY_FIELD = "keyField";
    public static final String ROW_KEY_VALUE = "my-key";
    public static final String STRING_FIELD = "stringField";
    public static final String STRING_VALUE = "some-string";
    public static final String INTEGER_FIELD = "integerField";
    public static final Integer INTEGER_VALUE = 1729;
    public static final Long TIMESTAMP = 1704067200000L;

    public static RowMutationEntry getTestRowMutationEntry() {
        RowMutationEntry entry = RowMutationEntry.create(ROW_KEY_VALUE);

        entry.setCell(
                        COLUMN_FAMILY,
                        ByteString.copyFromUtf8(STRING_FIELD),
                        TIMESTAMP,
                        ByteString.copyFrom(STRING_VALUE.getBytes()))
                .setCell(
                        COLUMN_FAMILY,
                        ByteString.copyFromUtf8(INTEGER_FIELD),
                        TIMESTAMP,
                        ByteString.copyFrom(
                                ByteBuffer.allocate(Integer.BYTES).putInt(INTEGER_VALUE).array()));
        return entry;
    }

    public static void assertRowMutationEntryEquality(
            RowMutationEntry serializedEntry, RowMutationEntry wantedEntry) {
        assertEquals(wantedEntry.toProto().getRowKey(), serializedEntry.toProto().getRowKey());
        assertEquals(
                wantedEntry.toProto().getMutationsCount(),
                serializedEntry.toProto().getMutationsCount());

        for (int i = 0; i < serializedEntry.toProto().getMutationsCount(); i++) {
            SetCell serializedCell = serializedEntry.toProto().getMutations(i).getSetCell();
            SetCell wantedCell = serializedEntry.toProto().getMutations(i).getSetCell();
            assertEquals(wantedCell.getColumnQualifier(), serializedCell.getColumnQualifier());
            assertEquals(wantedCell.getFamilyName(), serializedCell.getFamilyName());
            assertEquals(wantedCell.getValue(), serializedCell.getValue());
        }
    }
}
