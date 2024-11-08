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

package com.google.flink.connector.gcp.bigtable.serializers;

import org.apache.flink.util.function.SerializableFunction;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import org.junit.Test;

/**
 * Unit tests for the {@link FunctionRowMutationSerializer} class.
 *
 * <p>This class verifies the functionality of the {@link FunctionRowMutationSerializer} by testing
 * its ability to serialize different data types (String and Long) into {@link RowMutationEntry}
 * objects using provided functions.
 */
public class FunctionSerializerTest {

    private final SerializableFunction<String, RowMutationEntry> stringFunction =
            (s -> {
                String family = "family " + s;
                String qualifier = "qualifier " + s;
                String key = "key " + s;
                RowMutationEntry entry = RowMutationEntry.create(key).setCell(family, qualifier, s);
                return entry;
            });

    private final SerializableFunction<Long, RowMutationEntry> longFunction =
            (l -> {
                String family = "family " + l;
                String qualifier = "qualifier " + l;
                String key = "key " + l;
                RowMutationEntry entry = RowMutationEntry.create(key).setCell(family, qualifier, l);
                return entry;
            });

    @Test
    public void testCorrectRowMutationSerializationString() {
        FunctionRowMutationSerializer<String> serializerString =
                new FunctionRowMutationSerializer<String>(stringFunction);
        TestingUtils.assertRowMutationEntryEquality(
                stringFunction.apply("test"), serializerString.serialize("test", null));
    }

    @Test
    public void testCorrectRowMutationSerializationLong() {
        FunctionRowMutationSerializer<Long> serializerLong =
                new FunctionRowMutationSerializer<Long>(longFunction);
        TestingUtils.assertRowMutationEntryEquality(
                longFunction.apply(1729L), serializerLong.serialize(1729L, null));
    }

    @Test
    public void testStringFunction() {
        String s = "some-string";
        String family = "family " + s;
        String qualifier = "qualifier " + s;
        String key = "key " + s;
        RowMutationEntry entry = RowMutationEntry.create(key).setCell(family, qualifier, s);

        TestingUtils.assertRowMutationEntryEquality(entry, stringFunction.apply(s));
    }

    @Test
    public void testLongFunction() {
        Long l = 1729L;
        String family = "family " + l;
        String qualifier = "qualifier " + l;
        String key = "key " + l;
        RowMutationEntry entry = RowMutationEntry.create(key).setCell(family, qualifier, l);

        TestingUtils.assertRowMutationEntryEquality(entry, longFunction.apply(l));
    }
}
