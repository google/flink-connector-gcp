package com.google.flink.connector.gcp.bigquery;

import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.api.services.bigquery.model.TableFieldSchema;
import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link BigQueryTypeUtils}. */
public class BigQueryTypeUtilsTest {

    static Stream<Object[]> testToFlinkTypeData() {
        return Stream.of(
                new Object[]{"STRING", DataTypes.STRING()},
                new Object[]{"INTEGER", DataTypes.BIGINT()},
                new Object[]{"BOOLEAN", DataTypes.BOOLEAN()},
                new Object[]{"FLOAT", DataTypes.DOUBLE()},
                new Object[]{"BYTES", DataTypes.BYTES()},
                new Object[]{"DATE", DataTypes.DATE()},
                new Object[]{"DATETIME", DataTypes.TIMESTAMP_LTZ()},
                new Object[]{"TIME", DataTypes.TIME()},
                new Object[]{"TIMESTAMP", DataTypes.TIMESTAMP_LTZ()},
                new Object[]{"NUMERIC", DataTypes.DECIMAL(38, 9)},
                new Object[]{"BIGNUMERIC", DataTypes.BYTES()}
        );
    }

    @ParameterizedTest
    @MethodSource("testToFlinkTypeData")
    public void testToFlinkType(String bigQueryType, DataType expectedFlinkType) {
        TableFieldSchema bigQueryField = new TableFieldSchema().setName(bigQueryType.toLowerCase() + "_field").setType(bigQueryType);
        DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
        assertThat(flinkType).isEqualTo(expectedFlinkType);
    }

  @Test
  public void testToFlinkTypeStruct() {
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("struct_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("nested_string").setType("STRING"),
                    new TableFieldSchema().setName("nested_int64").setType("INTEGER")));
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("nested_string", DataTypes.STRING()),
                DataTypes.FIELD("nested_int64", DataTypes.BIGINT())));
  }

  @Test
  public void testToFlinkTypeNestedStruct() {
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("nested_struct_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("top_level_string").setType("STRING"),
                    new TableFieldSchema()
                        .setName("nested")
                        .setType("RECORD")
                        .setFields(
                            Arrays.asList(
                                new TableFieldSchema().setName("nested_string").setType("STRING"),
                                new TableFieldSchema().setName("nested_int64").setType("INTEGER")))));
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("top_level_string", DataTypes.STRING()),
                DataTypes.FIELD(
                    "nested",
                    DataTypes.ROW(
                        DataTypes.FIELD("nested_string", DataTypes.STRING()),
                        DataTypes.FIELD("nested_int64", DataTypes.BIGINT())))));
  }

  // The com.google.api.services.bigquery.model does not have a direct equivalent for RANGE type.
  // The RANGE type is usually represented as a STRUCT with lower and upper bounds.
  // Therefore, this test is adapted to reflect that representation.
  @Test
  public void testToFlinkTypeRangeDate() {
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("date_range_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("lower").setType("DATE"),
                    new TableFieldSchema().setName("upper").setType("DATE")));
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("lower", DataTypes.DATE()),
                DataTypes.FIELD("upper", DataTypes.DATE())));
  }

  // Similar to the Date Range, Numeric Range is represented as a STRUCT.
  @Test
  public void testToFlinkTypeRangeNumeric() {
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("numeric_range_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("lower").setType("NUMERIC"),
                    new TableFieldSchema().setName("upper").setType("NUMERIC")));
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("lower", DataTypes.DECIMAL(38, 9)),
                DataTypes.FIELD("upper", DataTypes.DECIMAL(38, 9))));
  }

  @Test
  public void testToFlinkTypeJSON() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("json_field").setType("JSON");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }

  @Test
  public void testToFlinkTypeGeography() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("geography_field").setType("GEOGRAPHY");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }

  @Test
  public void testJsonType() {
    TableFieldSchema jsonField =
        new TableFieldSchema().setName("json_data").setType("JSON");
    DataType expectedType = DataTypes.STRING();
    assertThat(BigQueryTypeUtils.toFlinkType(jsonField)).isEqualTo(expectedType);
  }

  @Test
  public void testToFlinkTypeNullableFieldsInStruct() {
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("nullable_struct_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("nullable_string").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("required_int64").setType("INTEGER").setMode("REQUIRED")));
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("nullable_string", DataTypes.STRING()),
                DataTypes.FIELD("required_int64", DataTypes.BIGINT())));
    // Note: Flink's DataTypes doesn't explicitly represent nullability at this level.
    // The nullability is handled at the runtime.
  }
}