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

package flink.connector.gcp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Creates load from a set list of words. */
public class WordLoadGenerator implements FlatMapFunction<Long, String> {
    private int load;
    private byte[] wordStr;
    static Path path = Paths.get("words.txt");

    public WordLoadGenerator(int l) {
        load = l;
        try {
            wordStr = Files.readAllBytes(path);
        } catch (Exception e) {
            System.out.println("Could not read words.txt file");
            wordStr =
                    ("Release, Ornamental, Cosmetic, Cement, Mud, Cleave, Zephyr, "
                                    + "Unusual, Receive, Atmosphere, Corrupt, Taboo, Cousin, Robotic, "
                                    + "Tramp, Heavyset, Current, Whisper, Alert, Approval, Forsake, Wind, "
                                    + "Consult, Women, Pitch, Easier, Shirk, Fighter, Disastrous, Basis, "
                                    + "Vanish, Freezing, Soar, Old-fashioned, Blankly, Closed, Parade, "
                                    + "Prophetic, Sponge, Moldy")
                            .getBytes();
        }
    }

    public byte[] makeBytesOfSize(int sizeBytes) {
        // since the bytes will be converted with UTF-16 encoding
        sizeBytes = sizeBytes / 2;
        byte[] result = new byte[sizeBytes];
        int current = 0;
        int remaining = sizeBytes;
        while (remaining > 0) {
            int len = Math.min(wordStr.length, remaining);
            System.arraycopy(wordStr, 0, result, current, len);
            current += len;
            remaining -= len;
        }
        return result;
    }

    public String randomWordStringOfSize(int sizeBytes) {
        return new String(makeBytesOfSize(sizeBytes));
    }

    @Override
    public void flatMap(Long value, Collector<String> out) {
        out.collect(randomWordStringOfSize(load));
    }
}
