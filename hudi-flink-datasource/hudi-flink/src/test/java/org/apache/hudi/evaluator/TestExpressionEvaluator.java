/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.evaluator;

import org.apache.hudi.utils.TestData;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.hudi.evaluator.ExpressionEvaluator.convertColumnStats;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ExpressionEvaluator}.
 */
public class TestExpressionEvaluator {
  private static final DataType ROW_DATA_TYPE = DataTypes.ROW(
      DataTypes.FIELD("f_tinyint", DataTypes.TINYINT()),
      DataTypes.FIELD("f_smallint", DataTypes.SMALLINT()),
      DataTypes.FIELD("f_int", DataTypes.INT()),
      DataTypes.FIELD("f_long", DataTypes.BIGINT()),
      DataTypes.FIELD("f_float", DataTypes.FLOAT()),
      DataTypes.FIELD("f_double", DataTypes.DOUBLE()),
      DataTypes.FIELD("f_boolean", DataTypes.BOOLEAN()),
      DataTypes.FIELD("f_decimal", DataTypes.DECIMAL(10, 2)),
      DataTypes.FIELD("f_bytes", DataTypes.VARBINARY(10)),
      DataTypes.FIELD("f_string", DataTypes.VARCHAR(10)),
      DataTypes.FIELD("f_time", DataTypes.TIME(3)),
      DataTypes.FIELD("f_date", DataTypes.DATE()),
      DataTypes.FIELD("f_timestamp", DataTypes.TIMESTAMP(3))
  ).notNull();
  private static final DataType INDEX_ROW_DATA_TYPE = DataTypes.ROW(
      DataTypes.FIELD("file_name", DataTypes.STRING()),
      DataTypes.FIELD("value_cnt", DataTypes.BIGINT()),
      DataTypes.FIELD("f_int_min", DataTypes.INT()),
      DataTypes.FIELD("f_int_max", DataTypes.INT()),
      DataTypes.FIELD("f_int_null_cnt", DataTypes.BIGINT()),
      DataTypes.FIELD("f_string_min", DataTypes.VARCHAR(10)),
      DataTypes.FIELD("f_string_max", DataTypes.VARCHAR(10)),
      DataTypes.FIELD("f_string_null_cnt", DataTypes.BIGINT()),
      DataTypes.FIELD("f_timestamp_min", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("f_timestamp_max", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("f_timestamp_null_cnt", DataTypes.BIGINT())
  ).notNull();

  private static final RowType INDEX_ROW_TYPE = (RowType) INDEX_ROW_DATA_TYPE.getLogicalType();

  @Test
  void testEqualTo() {
    ExpressionEvaluator.EqualTo equalTo = ExpressionEvaluator.EqualTo.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    equalTo.bindFieldReference(rExpr)
        .bindVal(vExpr);
    assertTrue(equalTo.eval(convertColumnStats(indexRow1, queryFields(2))), "11 < 12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    assertTrue(equalTo.eval(convertColumnStats(indexRow2, queryFields(2))), "12 <= 12 < 13");

    RowData indexRow3 = intIndexRow(11, 12);
    assertTrue(equalTo.eval(convertColumnStats(indexRow3, queryFields(2))), "11 < 12 <= 12");

    RowData indexRow4 = intIndexRow(10, 11);
    assertFalse(equalTo.eval(convertColumnStats(indexRow4, queryFields(2))), "11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    assertFalse(equalTo.eval(convertColumnStats(indexRow5, queryFields(2))), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    assertFalse(equalTo.eval(convertColumnStats(indexRow6, queryFields(2))), "12 <> null");

    equalTo.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(equalTo.eval(new HashMap<>()), "null <> null");
  }

  @Test
  void testNotEqualTo() {
    ExpressionEvaluator.NotEqualTo notEqualTo = ExpressionEvaluator.NotEqualTo.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    notEqualTo.bindFieldReference(rExpr)
        .bindVal(vExpr);
    assertTrue(notEqualTo.eval(convertColumnStats(indexRow1, queryFields(2))), "11 <> 12 && 12 <> 13");

    RowData indexRow2 = intIndexRow(12, 13);
    assertTrue(notEqualTo.eval(convertColumnStats(indexRow2, queryFields(2))), "12 <> 13");

    RowData indexRow3 = intIndexRow(11, 12);
    assertTrue(notEqualTo.eval(convertColumnStats(indexRow3, queryFields(2))), "11 <> 12");

    RowData indexRow4 = intIndexRow(10, 11);
    assertTrue(notEqualTo.eval(convertColumnStats(indexRow4, queryFields(2))), "10 <> 12 and 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    assertTrue(notEqualTo.eval(convertColumnStats(indexRow5, queryFields(2))), "12 <> 13 and 12 <> 14");

    RowData indexRow6 = intIndexRow(null, null);
    assertTrue(notEqualTo.eval(convertColumnStats(indexRow6, queryFields(2))), "12 <> null");

    notEqualTo.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(notEqualTo.eval(new HashMap<>()), "null <> null");
  }

  @Test
  void testIsNull() {
    ExpressionEvaluator.IsNull isNull = ExpressionEvaluator.IsNull.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    isNull.bindFieldReference(rExpr);
    assertTrue(isNull.eval(convertColumnStats(indexRow1, queryFields(2))), "2 nulls");

    RowData indexRow2 = intIndexRow(12, 13, 0L);
    assertFalse(isNull.eval(convertColumnStats(indexRow2, queryFields(2))), "0 nulls");
  }

  @Test
  void testIsNotNull() {
    ExpressionEvaluator.IsNotNull isNotNull = ExpressionEvaluator.IsNotNull.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    isNotNull.bindFieldReference(rExpr);
    assertTrue(isNotNull.eval(convertColumnStats(indexRow1, queryFields(2))), "min 11 is not null");

    RowData indexRow2 = intIndexRow(null, null, 0L);
    assertTrue(isNotNull.eval(convertColumnStats(indexRow2, queryFields(2))), "min is null and 0 nulls");
  }

  @Test
  void testLessThan() {
    ExpressionEvaluator.LessThan lessThan = ExpressionEvaluator.LessThan.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    lessThan.bindFieldReference(rExpr)
        .bindVal(vExpr);
    assertTrue(lessThan.eval(convertColumnStats(indexRow1, queryFields(2))), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    assertFalse(lessThan.eval(convertColumnStats(indexRow2, queryFields(2))), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    assertTrue(lessThan.eval(convertColumnStats(indexRow3, queryFields(2))), "11 < 12");

    RowData indexRow4 = intIndexRow(10, 11);
    assertTrue(lessThan.eval(convertColumnStats(indexRow4, queryFields(2))), "11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    assertFalse(lessThan.eval(convertColumnStats(indexRow5, queryFields(2))), "12 < min 13");

    RowData indexRow6 = intIndexRow(null, null);
    assertFalse(lessThan.eval(convertColumnStats(indexRow6, queryFields(2))), "12 <> null");

    lessThan.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(lessThan.eval(new HashMap<>()), "null <> null");
  }

  @Test
  void testGreaterThan() {
    ExpressionEvaluator.GreaterThan greaterThan = ExpressionEvaluator.GreaterThan.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    greaterThan.bindFieldReference(rExpr)
        .bindVal(vExpr);
    assertTrue(greaterThan.eval(convertColumnStats(indexRow1, queryFields(2))), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    assertTrue(greaterThan.eval(convertColumnStats(indexRow2, queryFields(2))), "12 < 13");

    RowData indexRow3 = intIndexRow(11, 12);
    assertFalse(greaterThan.eval(convertColumnStats(indexRow3, queryFields(2))), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    assertFalse(greaterThan.eval(convertColumnStats(indexRow4, queryFields(2))), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    assertTrue(greaterThan.eval(convertColumnStats(indexRow5, queryFields(2))), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    assertFalse(greaterThan.eval(convertColumnStats(indexRow6, queryFields(2))), "12 <> null");

    greaterThan.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(greaterThan.eval(new HashMap<>()), "null <> null");
  }

  @Test
  void testLessThanOrEqual() {
    ExpressionEvaluator.LessThanOrEqual lessThanOrEqual = ExpressionEvaluator.LessThanOrEqual.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    lessThanOrEqual.bindFieldReference(rExpr)
        .bindVal(vExpr);
    assertTrue(lessThanOrEqual.eval(convertColumnStats(indexRow1, queryFields(2))), "11 < 12");

    RowData indexRow2 = intIndexRow(12, 13);
    assertTrue(lessThanOrEqual.eval(convertColumnStats(indexRow2, queryFields(2))), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    assertTrue(lessThanOrEqual.eval(convertColumnStats(indexRow3, queryFields(2))), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    assertTrue(lessThanOrEqual.eval(convertColumnStats(indexRow4, queryFields(2))), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    assertFalse(lessThanOrEqual.eval(convertColumnStats(indexRow5, queryFields(2))), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    assertFalse(lessThanOrEqual.eval(convertColumnStats(indexRow6, queryFields(2))), "12 <> null");

    lessThanOrEqual.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(lessThanOrEqual.eval(new HashMap<>()), "null <> null");
  }

  @Test
  void testGreaterThanOrEqual() {
    ExpressionEvaluator.GreaterThanOrEqual greaterThanOrEqual = ExpressionEvaluator.GreaterThanOrEqual.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    greaterThanOrEqual.bindFieldReference(rExpr)
        .bindVal(vExpr);
    assertTrue(greaterThanOrEqual.eval(convertColumnStats(indexRow1, queryFields(2))), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    assertTrue(greaterThanOrEqual.eval(convertColumnStats(indexRow2, queryFields(2))), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    assertTrue(greaterThanOrEqual.eval(convertColumnStats(indexRow3, queryFields(2))), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    assertFalse(greaterThanOrEqual.eval(convertColumnStats(indexRow4, queryFields(2))), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    assertTrue(greaterThanOrEqual.eval(convertColumnStats(indexRow5, queryFields(2))), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    assertFalse(greaterThanOrEqual.eval(convertColumnStats(indexRow6, queryFields(2))), "12 <> null");

    greaterThanOrEqual.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(greaterThanOrEqual.eval(new HashMap<>()), "null <> null");
  }

  @Test
  void testIn() {
    ExpressionEvaluator.In in = ExpressionEvaluator.In.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    in.bindFieldReference(rExpr);
    in.bindVals(12);
    assertTrue(in.eval(convertColumnStats(indexRow1, queryFields(2))), "11 < 12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    assertTrue(in.eval(convertColumnStats(indexRow2, queryFields(2))), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    assertTrue(in.eval(convertColumnStats(indexRow3, queryFields(2))), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    assertFalse(in.eval(convertColumnStats(indexRow4, queryFields(2))), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    assertFalse(in.eval(convertColumnStats(indexRow5, queryFields(2))), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    assertFalse(in.eval(convertColumnStats(indexRow6, queryFields(2))), "12 <> null");

    in.bindVals((Object) null);
    assertFalse(in.eval(new HashMap<>()), "null <> null");
  }

  private static RowData intIndexRow(Integer minVal, Integer maxVal) {
    return intIndexRow(minVal, maxVal, 2L);
  }

  private static RowData intIndexRow(Integer minVal, Integer maxVal, Long nullCnt) {
    return indexRow(StringData.fromString("f1"), 100L,
        minVal, maxVal, nullCnt,
        StringData.fromString("1"), StringData.fromString("100"), 5L,
        TimestampData.fromEpochMillis(1), TimestampData.fromEpochMillis(100), 3L);
  }

  private static RowData indexRow(Object... fields) {
    return TestData.insertRow(INDEX_ROW_TYPE, fields);
  }

  private static RowType.RowField[] queryFields(int... pos) {
    List<RowType.RowField> fields = ((RowType) ROW_DATA_TYPE.getLogicalType()).getFields();
    return Arrays.stream(pos).mapToObj(fields::get).toArray(RowType.RowField[]::new);
  }
}
