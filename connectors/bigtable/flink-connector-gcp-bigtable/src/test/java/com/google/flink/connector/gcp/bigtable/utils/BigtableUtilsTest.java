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

import org.apache.flink.api.connector.sink2.SinkWriter;

import org.junit.Test;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link BigtableUtils} class.
 *
 * <p>This class verifies the functionality of the {@link
 * BigtableUtils#getTimestamp(SinkWriter.Context)} method, ensuring that it correctly handles
 * various scenarios, including valid timestamps, null contexts, and invalid timestamps (null,
 * negative, zero).
 */
public class BigtableUtilsTest {

    // Allowed delta time for tests
    private static final Long ALLOWED_DELTA = 100000L; // 0.1 second

    SinkWriter.Context mockContext = mock(SinkWriter.Context.class);

    @Test
    public void testGetTimestamp() {
        Long contextTimestampMillis = 1700000000L;
        Long contextTimestampMicros = contextTimestampMillis * 1000;
        Mockito.when(mockContext.timestamp()).thenReturn(contextTimestampMillis);
        assertEquals(contextTimestampMicros, BigtableUtils.getTimestamp(mockContext));
    }

    @Test
    public void testNullContext() {
        Long currentMicros = Instant.now().toEpochMilli() * 1000;
        assertThat(BigtableUtils.getTimestamp(null))
                .isBetween(currentMicros, currentMicros + ALLOWED_DELTA);
    }

    @ParameterizedTest
    @MethodSource("timestampCases")
    public void testTimestampValidation(Long mockedTimestamp) {
        Long currentMicros = Instant.now().toEpochMilli() * 1000;
        Mockito.when(mockContext.timestamp()).thenReturn(mockedTimestamp);
        assertThat(BigtableUtils.getTimestamp(mockContext))
                .isBetween(currentMicros, currentMicros + ALLOWED_DELTA);
    }

    private static Stream<Arguments> timestampCases() {
        return Stream.of(
                Arguments.of(Named.of("Null Timestamp", null)),
                Arguments.of(Named.of("Negative Timestamp", -1234L)),
                Arguments.of(Named.of("Zero Timestamp", 0L)));
    }
}
