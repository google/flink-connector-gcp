package com.google.flink.connector.gcp.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import java.util.Arrays;

import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link BigQueryTypeUtils}. */
public class BigQueryTypeUtilsTest {

  @Test
  public void testToFlinkTypeString() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("string_field").setType("STRING");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }

  @Test
  public void testToFlinkTypeInt64() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("int64_field").setType("INTEGER");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BIGINT());
  }

  @Test
  public void testToFlinkTypeBool() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("bool_field").setType("BOOLEAN");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BOOLEAN());
  }

  @Test
  public void testToFlinkTypeFloat64() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("float64_field").setType("FLOAT");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DOUBLE());
  }

  @Test
  public void testToFlinkTypeBytes() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("bytes_field").setType("BYTES");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BYTES());
  }

  @Test
  public void testToFlinkTypeDate() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("date_field").setType("DATE");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DATE());
  }

  @Test
  public void testToFlinkTypeDatetime() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("datetime_field").setType("DATETIME");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIMESTAMP_LTZ());
  }

  @Test
  public void testToFlinkTypeTime() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("time_field").setType("TIME");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIME());
  }

  @Test
  public void testToFlinkTypeTimestamp() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("timestamp_field").setType("TIMESTAMP");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIMESTAMP_LTZ());
  }

  @Test
  public void testToFlinkTypeNumeric() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("numeric_field").setType("NUMERIC");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DECIMAL(38, 9));
  }

  @Test
  public void testToFlinkTypeBignumeric() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("bignumeric_field").setType("BIGNUMERIC");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BYTES());
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