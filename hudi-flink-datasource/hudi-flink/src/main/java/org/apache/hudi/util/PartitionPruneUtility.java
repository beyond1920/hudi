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

package org.apache.hudi.util;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The utility for extracting partition prune filters from all push down filters.
 */
public class PartitionPruneUtility {

  /**
   * Collects partition filters from given expressions, only simple call expression is supported.
   */
  public static List<CallExpression> extractAndTransformPartitionFilter(
      List<ResolvedExpression> exprs,
      List<String> partitionKeys,
      RowType tableRowType) {
    if (partitionKeys.isEmpty()) {
      return Collections.emptyList();
    }
    int[] partitionIdxMapping = tableRowType.getFieldNames().stream()
        .mapToInt(partitionKeys::indexOf).toArray();
    List<CallExpression> partitionFilters = new ArrayList<>();
    for (ResolvedExpression expr : exprs) {
      for (CallExpression e : splitByAnd(expr)) {
        CallExpression convertedExpr = applyMapping(e, partitionIdxMapping);
        if (convertedExpr != null) {
          partitionFilters.add(convertedExpr);
        }
      }
    }
    return partitionFilters;
  }

  private static List<CallExpression> splitByAnd(ResolvedExpression expr) {
    List<CallExpression> result = new ArrayList<>();
    splitByAnd(expr, result);
    return result;
  }

  private static void splitByAnd(
      ResolvedExpression expr,
      List<CallExpression> result) {
    if (!(expr instanceof CallExpression)) {
      return;
    }
    CallExpression callExpr = (CallExpression) expr;
    FunctionDefinition funcDef = callExpr.getFunctionDefinition();

    if (funcDef == BuiltInFunctionDefinitions.AND) {
      callExpr.getChildren().stream()
          .filter(child -> child instanceof CallExpression)
          .forEach(child -> splitByAnd((CallExpression) child, result));
    } else if (funcDef == BuiltInFunctionDefinitions.IN
        || funcDef == BuiltInFunctionDefinitions.EQUALS
        || funcDef == BuiltInFunctionDefinitions.NOT_EQUALS
        || funcDef == BuiltInFunctionDefinitions.IS_NULL
        || funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL
        || funcDef == BuiltInFunctionDefinitions.LESS_THAN
        || funcDef == BuiltInFunctionDefinitions.GREATER_THAN
        || funcDef == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL
        || funcDef == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
      result.add(callExpr);
    }
  }

  private static CallExpression applyMapping(CallExpression expr, int[] fieldIdxMapping) {
    FunctionDefinition funcDef = expr.getFunctionDefinition();
    if (funcDef == BuiltInFunctionDefinitions.IN
        || funcDef == BuiltInFunctionDefinitions.EQUALS
        || funcDef == BuiltInFunctionDefinitions.NOT_EQUALS
        || funcDef == BuiltInFunctionDefinitions.IS_NULL
        || funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL
        || funcDef == BuiltInFunctionDefinitions.LESS_THAN
        || funcDef == BuiltInFunctionDefinitions.GREATER_THAN
        || funcDef == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL
        || funcDef == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
      List<Expression> children = expr.getChildren();
      List<ResolvedExpression> newChildren = children.stream()
          .map(
              child -> {
                if (child instanceof FieldReferenceExpression) {
                  FieldReferenceExpression refExpr = (FieldReferenceExpression) child;
                  int target = fieldIdxMapping[refExpr.getFieldIndex()];
                  if (target >= 0) {
                    return new FieldReferenceExpression(
                        refExpr.getName(),
                        refExpr.getOutputDataType(),
                        refExpr.getInputIndex(),
                        target);
                  } else {
                    return null;
                  }
                } else {
                  return (ResolvedExpression) child;
                }
              })
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
      if (newChildren.size() == children.size()) {
        return expr.replaceArgs(newChildren, expr.getOutputDataType());
      } else {
        return null;
      }
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + funcDef);
    }
  }
}
