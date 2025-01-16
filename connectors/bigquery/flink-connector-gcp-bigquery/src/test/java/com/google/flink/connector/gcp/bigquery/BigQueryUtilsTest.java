package com.google.flink.connector.gcp.bigquery;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldElementType;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link BigQueryTypeUtils}. */
public class BigQueryUtilsTest {

  @Test
  public void testToFlinkTypeString() {
    Field bigQueryField = Field.newBuilder("string_field", StandardSQLTypeName.STRING).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }

  @Test
  public void testToFlinkTypeInt64() {
    Field bigQueryField = Field.newBuilder("int64_field", StandardSQLTypeName.INT64).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BIGINT());
  }

  @Test
  public void testToFlinkTypeBool() {
    Field bigQueryField = Field.newBuilder("bool_field", StandardSQLTypeName.BOOL).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BOOLEAN());
  }

  @Test
  public void testToFlinkTypeFloat64() {
    Field bigQueryField = Field.newBuilder("float64_field", StandardSQLTypeName.FLOAT64).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DOUBLE());
  }

  @Test
  public void testToFlinkTypeBytes() {
    Field bigQueryField = Field.newBuilder("bytes_field", StandardSQLTypeName.BYTES).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BYTES());
  }

  @Test
  public void testToFlinkTypeDate() {
    Field bigQueryField = Field.newBuilder("date_field", StandardSQLTypeName.DATE).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DATE());
  }

  @Test
  public void testToFlinkTypeDatetime() {
    Field bigQueryField = Field.newBuilder("datetime_field", StandardSQLTypeName.DATETIME).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIMESTAMP_LTZ());
  }

  @Test
  public void testToFlinkTypeTime() {
    Field bigQueryField = Field.newBuilder("time_field", StandardSQLTypeName.TIME).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIME());
  }

  @Test
  public void testToFlinkTypeTimestamp() {
    Field bigQueryField = Field.newBuilder("timestamp_field", StandardSQLTypeName.TIMESTAMP).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIMESTAMP_LTZ());
  }

  @Test
  public void testToFlinkTypeNumeric() {
    Field bigQueryField = Field.newBuilder("numeric_field", StandardSQLTypeName.NUMERIC).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DECIMAL(38, 9));
  }

  @Test
  public void testToFlinkTypeBignumeric() {
    Field bigQueryField = Field.newBuilder("bignumeric_field", StandardSQLTypeName.BIGNUMERIC).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BYTES());
  }

  @Test
  public void testToFlinkTypeStruct() {
    Field bigQueryField =
        Field.newBuilder(
                "struct_field",
                StandardSQLTypeName.STRUCT,
                FieldList.of(
                    Field.of("nested_string", StandardSQLTypeName.STRING),
                    Field.of("nested_int64", StandardSQLTypeName.INT64)))
            .build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("nested_string", DataTypes.STRING()),
                DataTypes.FIELD("nested_int64", DataTypes.BIGINT())));
  }

  @Test
  public void testToFlinkTypeNestedStruct() {
    Field bigQueryField =
        Field.newBuilder(
                "nested_struct_field",
                StandardSQLTypeName.STRUCT,
                FieldList.of(
                    Field.of("top_level_string", StandardSQLTypeName.STRING),
                    Field.newBuilder(
                            "nested",
                            StandardSQLTypeName.STRUCT,
                            FieldList.of(
                                Field.of("nested_string", StandardSQLTypeName.STRING),
                                Field.of("nested_int64", StandardSQLTypeName.INT64)))
                        .build()))
            .build();
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

  @Test
  public void testToFlinkTypeRangeDate() {
    Field bigQueryField =
        Field.newBuilder("date_range_field", StandardSQLTypeName.RANGE)
            .setRangeElementType(FieldElementType.newBuilder().setType(StandardSQLTypeName.DATE.name()).build())
            .build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("lower", DataTypes.DATE()),
                DataTypes.FIELD("upper", DataTypes.DATE())));
  }

  @Test
  public void testToFlinkTypeRangeNumeric() {
    Field bigQueryField =
        Field.newBuilder("numeric_range_field", StandardSQLTypeName.RANGE)
            .setRangeElementType(FieldElementType.newBuilder().setType(StandardSQLTypeName.NUMERIC.name()).build())
            .build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("lower", DataTypes.DECIMAL(38, 9)),
                DataTypes.FIELD("upper", DataTypes.DECIMAL(38, 9))));
  }

  @Test
  public void testToFlinkTypeJSON() {
    Field bigQueryField = Field.newBuilder("json_field", StandardSQLTypeName.JSON).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }


  @Test
  public void testToFlinkTypeGeography() {
    Field bigQueryField = Field.newBuilder("geography_field", StandardSQLTypeName.GEOGRAPHY).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }
  
  @Test
  public void testJsonType() {
    Field jsonField =
        Field.newBuilder("json_data", StandardSQLTypeName.JSON).build();
    DataType expectedType = DataTypes.STRING();
    assertThat(BigQueryTypeUtils.toFlinkType(jsonField)).isEqualTo(expectedType);
  }

  @Test
  public void testToFlinkTypeNullableFieldsInStruct() {
    Field bigQueryField =
        Field.newBuilder(
                "nullable_struct_field",
                StandardSQLTypeName.STRUCT,
                FieldList.of(
                    Field.newBuilder("nullable_string", StandardSQLTypeName.STRING)
                        .setMode(Mode.NULLABLE)
                        .build(),
                    Field.newBuilder("required_int64", StandardSQLTypeName.INT64)
                        .setMode(Mode.REQUIRED)
                        .build()))
            .build();
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