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

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.keygen.CustomAvroKeyGenerator.SPLIT_REGEX;

/**
 * Auto record key generator. This generator will fetch values from the entire record based on some of the fields and determine the record key.
 * Use-cases where users may not be able to configure record keys, can use this auto record key generator.
 */
public class AutoRecordKeyGenerator implements RecordKeyGenerator {

  private final TypedProperties config;
  private static final String HOODIE_PREFIX = "_hoodie";
  private static final String DOT = ".";
  private final int maxFieldsToConsider;
  private final int numFieldsForKey;
  private final Set<String> partitionFieldNames;
  private int[][] fieldOrdering;

  public AutoRecordKeyGenerator(TypedProperties config, List<String> partitionPathFields) {
    this.config = config;
    this.numFieldsForKey = config.getInteger(KeyGeneratorOptions.NUM_FIELDS_IN_KEYLESS_GENERATOR.key(), KeyGeneratorOptions.NUM_FIELDS_IN_KEYLESS_GENERATOR.defaultValue());
    // cap the number of fields to order in case of large schemas
    this.maxFieldsToConsider = numFieldsForKey * 3;
    this.partitionFieldNames = partitionPathFields.stream().map(field -> field.split(SPLIT_REGEX)[0]).collect(Collectors.toSet());
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return buildKey(getFieldOrdering(record), record);
  }

  int[][] getFieldOrdering(GenericRecord genericRecord) {
    if (fieldOrdering == null) {
      fieldOrdering = buildFieldOrdering(genericRecord.getSchema().getFields());
    }
    return fieldOrdering;
  }

  /**
   * Deterministically builds a key for the input value based on the provided fieldOrdering. The first {@link #numFieldsForKey} non-null values will be used to generate a string that is passed to
   * {@link UUID#nameUUIDFromBytes(byte[])}.
   * @param fieldOrdering an array of integer arrays. The integer arrays represent paths to a single field within the input object.
   * @param input the input object that needs a key
   * @return a deterministically generated {@link UUID}
   * @param <T> the input object type
   */
  private <T> String buildKey(int[][] fieldOrdering, GenericRecord input) {
    StringBuilder key = new StringBuilder();
    int nonNullFields = 0;
    for (int[] index : fieldOrdering) {
      Object value = getFieldForRecord(input, index);
      if (value == null) {
        continue;
      }
      nonNullFields++;
      key.append(value.hashCode());
      if (nonNullFields >= numFieldsForKey) {
        break;
      }
    }
    return UUID.nameUUIDFromBytes(key.toString().getBytes(StandardCharsets.UTF_8)).toString();
  }

  /**
   * Gets the value of the field at the specified path within the record.
   * @param record the input record
   * @param fieldPath the path to the field as an array of integers representing the field position within the object
   * @return value at the path
   */
  private static Object getFieldForRecord(GenericRecord record, int[] fieldPath) {
    Object value = record;
    for (Integer index : fieldPath) {
      if (value == null) {
        return null;
      }
      value = ((GenericRecord) value).get(index);
    }
    return value;
  }

  private int[][] buildFieldOrdering(List<Schema.Field> initialFields) {
    PriorityQueue<Pair<int[], Integer>> queue = new PriorityQueue<>(maxFieldsToConsider + 1, RankingComparator.getInstance());
    Queue<FieldToProcess> fieldsToProcess = new ArrayDeque<>();
    for (int j = 0; j < initialFields.size(); j++) {
      fieldsToProcess.offer(new FieldToProcess(new int[]{j}, initialFields.get(j), initialFields.get(j).name()));
    }
    while (!fieldsToProcess.isEmpty()) {
      FieldToProcess fieldToProcess = fieldsToProcess.poll();
      int[] existingPath = fieldToProcess.getIndexPath();
      Schema fieldSchema = fieldToProcess.getField().schema();
      if (fieldSchema.getType() == Schema.Type.UNION) {
        fieldSchema = fieldSchema.getTypes().get(1);
      }
      if (fieldSchema.getType() == Schema.Type.RECORD) {
        List<Schema.Field> nestedFields = fieldSchema.getFields();
        for (int i = 0; i < nestedFields.size(); i++) {
          int[] path = Arrays.copyOf(existingPath, existingPath.length + 1);
          path[existingPath.length] = i;
          Schema.Field nestedField = nestedFields.get(i);
          fieldsToProcess.add(new FieldToProcess(path, nestedField, fieldToProcess.getNamePath() + DOT + nestedField.name()));
        }
      } else {
        // check that field is not used in partitioning
        if (!partitionFieldNames.contains(fieldToProcess.getNamePath())) {
          queue.offer(Pair.of(existingPath, getSchemaRanking(fieldToProcess.getField())));
          if (queue.size() > maxFieldsToConsider) {
            queue.poll();
          }
        }
      }
    }
    Pair<int[], Integer>[] sortedPairs = queue.toArray(new Pair[queue.size()]);
    Arrays.sort(sortedPairs, RankingComparator.getInstance().reversed());
    int[][] output = new int[sortedPairs.length][];
    for (int k = 0; k < sortedPairs.length; k++) {
      output[k] = sortedPairs[k].getLeft();
    }
    return output;
  }

  private static class FieldToProcess {
    final int[] indexPath;
    final Schema.Field field;
    final String namePath;

    public FieldToProcess(int[] indexPath, Schema.Field field, String namePath) {
      this.indexPath = indexPath;
      this.field = field;
      this.namePath = namePath;
    }

    public int[] getIndexPath() {
      return indexPath;
    }

    public Schema.Field getField() {
      return field;
    }

    public String getNamePath() {
      return namePath;
    }
  }

  /**
   * Ranks the fields by their type.
   * @param field input field
   * @return a score of 0 to 4
   */
  private int getSchemaRanking(Schema.Field field) {
    if (field.name().startsWith(HOODIE_PREFIX)) {
      return 0;
    }
    Schema schema = field.schema();
    if (schema.getType() == Schema.Type.UNION) {
      schema = schema.getTypes().get(0).getType() == Schema.Type.NULL ? schema.getTypes().get(1) : schema.getTypes().get(0);
    }
    Schema.Type type = schema.getType();
    switch (type) {
      case LONG:
        // assumes long with logical type will be a timestamp
        return schema.getLogicalType() != null ? 4 : 3;
      case INT:
        // assumes long with logical type will be a date which will have low variance in a batch
        return schema.getLogicalType() != null ? 1 : 3;
      case DOUBLE:
      case FLOAT:
        return 3;
      case BOOLEAN:
      case MAP:
      case ARRAY:
        return 1;
      default:
        return 2;
    }
  }

  private static class RankingComparator implements Comparator<Pair<int[], Integer>> {
    private static final RankingComparator INSTANCE = new RankingComparator();

    static RankingComparator getInstance() {
      return INSTANCE;
    }

    @Override
    public int compare(Pair<int[], Integer> o1, Pair<int[], Integer> o2) {
      int initialResult = o1.getRight().compareTo(o2.getRight());
      if (initialResult == 0) {
        // favor the smaller list (less nested value) on ties
        int sizeResult = Integer.compare(o2.getLeft().length, o1.getLeft().length);
        if (sizeResult == 0) {
          return Integer.compare(o2.getLeft()[0], o1.getLeft()[0]);
        }
        return sizeResult;
      }
      return initialResult;
    }
  }
}
