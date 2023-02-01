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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.evaluator.ExpressionEvaluator;

import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for incremental partition pruning.
 */
public class PartitionPruner implements Serializable {

  private static final long serialVersionUID = 1L;

  private final List<ExpressionEvaluator.Evaluator> partitionEvaluators;

  private final List<String> partitionKeys;

  private final List<DataType> partitionTypes;

  private final String defaultParName;

  private final boolean hivePartition;

  public PartitionPruner(
      List<ExpressionEvaluator.Evaluator> partitionEvaluators,
      List<String> partitionKeys,
      List<DataType> partitionTypes,
      String defaultParName,
      boolean hivePartition) {
    this.partitionEvaluators = partitionEvaluators;
    this.partitionKeys = partitionKeys;
    this.partitionTypes = partitionTypes;
    this.defaultParName = defaultParName;
    this.hivePartition = hivePartition;
  }

  public Set<String> apply(Set<String> partitions) {
    int partitionKeyCnt = partitionKeys.size();
    return partitions.stream()
        .filter(partition -> {
          String[] partitionStrArray = extractPartitionValues(partition, partitionKeys, hivePartition);
          Object[] partitionVals = new Object[partitionKeyCnt];
          for (int idx = 0; idx < partitionKeyCnt; idx++) {
            if (partitionKeys.get(idx).equals(defaultParName)) {
              partitionVals[idx] = null;
            } else {
              partitionVals[idx] = DataTypeUtils.resolvePartition(
                  partitionStrArray[idx], partitionTypes.get(idx));
            }
          }
          for (ExpressionEvaluator.Evaluator evaluator : partitionEvaluators) {
            if (!evaluator.eval(partitionVals)) {
              return false;
            }
          }
          return true;
        }).collect(Collectors.toSet());
  }

  private static String[] extractPartitionValues(
      String partition,
      List<String> partitionKeys,
      boolean hivePartition) {
    String[] paths = partition.split(Path.SEPARATOR);
    ValidationUtils.checkArgument(
        paths.length == partitionKeys.size(),
        "Illegal partition: " + partition);
    if (hivePartition) {
      String[] partitionValues = new String[paths.length];
      for (int idx = 0; idx < paths.length; idx++) {
        String[] kv = paths[idx].split("=");
        ValidationUtils.checkArgument(
            kv.length == 2 && kv[0].equals(partitionKeys.get(idx)),
            "Illegal partition: " + partition);
        partitionValues[idx] = kv[1];
      }
      return partitionValues;
    } else {
      return paths;
    }
  }
}
