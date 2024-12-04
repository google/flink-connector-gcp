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

package flink.connector.gcp.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * A Flink User-Defined Table Function (UDTF) for splitting strings.
 *
 * <p>This UDTF accepts a string and a delimiter as input and produces zero or more rows,
 * each containing a single split value from the input string.
 *
 * <h3>Usage in Flink SQL:</h3>
 * <ol>
 *   <li>Register the UDTF:
 *     <pre><code>
 *     CREATE TEMPORARY FUNCTION split_string AS 'com.yourpackage.StringSplitUDTF';
 *     </code></pre>
 *   </li>
 *   <li>Use in a query with LATERAL TABLE:
 *     <pre><code>
 *     SELECT original_string, split_value
 *     FROM your_table,
 *     LATERAL TABLE(split_string(your_table.string_column, ',')) AS T(split_value);
 *     </code></pre>
 *   </li>
 * </ol>
 *
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class StringSplitUDTF extends TableFunction<Row> {

  /**
   * Evaluates the input string and delimiter, splitting the string and emitting the resulting values.
   *
   * @param inputString The string to be split.
   * @param delimiter The delimiter used for splitting.
   */
  public void eval(String inputString, String delimiter) {
    if (inputString != null && delimiter != null) {
      String[] splitValues = inputString.split(delimiter);
      for (String value : splitValues) {
        collect(Row.of(value));
      }
    }
  }
}
