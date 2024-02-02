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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.expression.NameReference;
import org.apache.hudi.expression.BinaryExpression;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_FILTER_PUSHDOWN_MAX_SIZE;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;

public class PartitionFilterGenerator {

  private static final Set<String> SUPPORT_TYPES = new HashSet<String>() {
    {
      add(HiveSchemaUtil.INT_TYPE_NAME);
      add(HiveSchemaUtil.BIGINT_TYPE_NAME);
      add(HiveSchemaUtil.DATE_TYPE_NAME);
      add(HiveSchemaUtil.STRING_TYPE_NAME);
    }
  };

  private static final String UNSUPPORTED_TYPE_ERROR = "The value type: %s doesn't support to "
      + "be pushed down to HMS, acceptable types: " + String.join(",", SUPPORT_TYPES);

  private Literal buildLiteralExpression(String fieldValue, String fieldType) {
    switch (fieldType.toLowerCase(Locale.ROOT)) {
      case HiveSchemaUtil.INT_TYPE_NAME:
        return new Literal<>(Integer.parseInt(fieldValue), Types.IntType.get());
      case HiveSchemaUtil.BIGINT_TYPE_NAME:
        return new Literal<>(Long.parseLong(fieldValue), Types.LongType.get());
      // TODO Handle Date value
      case HiveSchemaUtil.DATE_TYPE_NAME:
        return new Literal<>(fieldValue, Types.DateType.get());
      case HiveSchemaUtil.STRING_TYPE_NAME:
        return new Literal<>(fieldValue, Types.StringType.get());
      case HiveSchemaUtil.BOOLEAN_TYPE_NAME:
        return new Literal<>(Boolean.parseBoolean(fieldValue), Types.BooleanType.get());
      default:
        throw new IllegalArgumentException(String.format(UNSUPPORTED_TYPE_ERROR, fieldType));
    }
  }


  /**
   * Build expression from the Partition list. Here we're trying to match all partitions.
   *
   * ex. partitionSchema(date, hour) [Partition(2022-09-01, 12), Partition(2022-09-02, 13)] =>
   *     Or(And(Equal(Attribute(date), Literal(2022-09-01)), Equal(Attribute(hour), Literal(12))),
   *     And(Equal(Attribute(date), Literal(2022-09-02)), Equal(Attribute(hour), Literal(13))))
   */
  private Expression buildPartitionExpression(List<Partition> partitions, List<FieldSchema> partitionFields) {
    return partitions.stream().map(partition -> {
      List<String> partitionValues = partition.getValues();
      Expression root = null;

      for (int i = 0; i < partitionFields.size(); i++) {
        FieldSchema field = partitionFields.get(i);
        BinaryExpression exp = Predicates.eq(new NameReference(field.getName()),
            buildLiteralExpression(partitionValues.get(i), field.getType()));
        if (root != null) {
          root = Predicates.and(root, exp);
        } else {
          root = exp;
        }
      }
      return root;
    }).reduce(null, (result, expr) -> {
      if (result == null) {
        return expr;
      } else {
        return Predicates.or(result, expr);
      }
    });
  }

  /**
   * Extract partition values from the {@param partitions}, and binding to
   * corresponding partition fieldSchemas.
   */
  private List<Pair<FieldSchema, String[]>> extractFieldValues(List<Partition> partitions, List<FieldSchema> partitionFields) {
    return IntStream.range(0, partitionFields.size())
        .mapToObj(i -> {
          Set<String> values = new HashSet<String>();
          for (int j = 0; j < partitions.size(); j++) {
            values.add(partitions.get(j).getValues().get(i));
          }
          return Pair.of(partitionFields.get(i), values.toArray(new String[0]));
        })
        .collect(Collectors.toList());
  }

  private class ValueComparator implements Comparator<String> {

    private final String valueType;
    public ValueComparator(String type) {
      this.valueType = type;
    }

    /**
     * As HMS only accept DATE, INT, STRING, BIGINT to push down partition filters, here we only
     * do the comparison for these types.
     */
    @Override
    public int compare(String s1, String s2) {
      switch (valueType.toLowerCase(Locale.ROOT)) {
        case HiveSchemaUtil.INT_TYPE_NAME:
          int i1 = Integer.parseInt(s1);
          int i2 = Integer.parseInt(s2);
          return i1 - i2;
        case HiveSchemaUtil.BIGINT_TYPE_NAME:
          long l1 = Long.parseLong(s1);
          long l2 = Long.parseLong(s2);
          return Long.signum(l1 - l2);
        case HiveSchemaUtil.DATE_TYPE_NAME:
        case HiveSchemaUtil.STRING_TYPE_NAME:
          return s1.compareTo(s2);
        default:
          throw new IllegalArgumentException(String.format(UNSUPPORTED_TYPE_ERROR, valueType));
      }
    }
  }

  /**
   * This method will extract the min value and the max value of each field,
   * and construct GreatThanOrEqual and LessThanOrEqual to build the expression.
   *
   * This method can reduce the Expression tree level a lot if each field has too many values.
   */
  private Expression buildMinMaxPartitionExpression(List<Partition> partitions, List<FieldSchema> partitionFields) {
    return extractFieldValues(partitions, partitionFields).stream().map(fieldWithValues -> {
      FieldSchema fieldSchema = fieldWithValues.getKey();

      if (!SUPPORT_TYPES.contains(fieldSchema.getType())) {
        return null;
      }

      String[] values = fieldWithValues.getValue();

      if (values.length == 1) {
        return Predicates.eq(new NameReference(fieldSchema.getName()),
            buildLiteralExpression(values[0], fieldSchema.getType()));
      }

      Arrays.sort(values, new ValueComparator(fieldSchema.getType()));

      return Predicates.and(
          Predicates.gteq(
              new NameReference(fieldSchema.getName()),
              buildLiteralExpression(values[0], fieldSchema.getType())),
          Predicates.lteq(
              new NameReference(fieldSchema.getName()),
              buildLiteralExpression(values[values.length - 1], fieldSchema.getType())));
    })
        .filter(Objects::nonNull)
        .reduce(null, (result, expr) -> {
          if (result == null) {
            return expr;
          } else {
            return Predicates.and(result, expr);
          }
        });
  }

  public  String generatePushDownFilter(List<String> writtenPartitions, List<FieldSchema> partitionFields, HiveSyncConfig config) {
    PartitionValueExtractor partitionValueExtractor = ReflectionUtils
        .loadClass(config.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS));

    List<Partition> partitions = writtenPartitions.stream().map(s -> {
      List<String> values = partitionValueExtractor.extractPartitionValuesInPath(s);

      if (values.size() != partitionFields.size()) {
        throw new HoodieHiveSyncException("Partition fields and values should be same length"
            + ", but got partitionFields: " + partitionFields + " with values: " + values);
      }

      return new Partition(values, null);
    }).collect(Collectors.toList());

    Expression filter;
    int estimateSize = partitionFields.size() * partitions.size();
    if (estimateSize > config.getIntOrDefault(HIVE_SYNC_FILTER_PUSHDOWN_MAX_SIZE)) {
      filter = buildMinMaxPartitionExpression(partitions, partitionFields);
    } else {
      filter = buildPartitionExpression(partitions, partitionFields);
    }

    if (filter != null) {
      return generateFilterString(filter);
    }

    return "";
  }

  protected String generateFilterString(Expression filter) {
    return filter.accept(new FilterGenVisitor());
  }
}
