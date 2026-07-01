/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bulk.sort;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.util.Arrays;

/**
 * Tools to generate the sort operator.
 */
public class SortOperatorGen {
  private final int[] sortIndices;
  private final RowType rowType;
  private final RowData.FieldGetter[] fieldGetters;

  public SortOperatorGen(RowType rowType, String[] sortFields) {
    this.rowType = rowType;
    this.sortIndices = Arrays.stream(sortFields).mapToInt(field -> {
      int index = rowType.getFieldIndex(field);
      if (index < 0) {
        throw new IllegalArgumentException("Can not find sort field '" + field + "' in row type " + rowType);
      }
      return index;
    }).toArray();
    this.fieldGetters = Arrays.stream(sortIndices)
        .mapToObj(index -> RowData.createFieldGetter(rowType.getTypeAt(index), index))
        .toArray(RowData.FieldGetter[]::new);
  }

  public OneInputStreamOperator<RowData, RowData> createSortOperator(Configuration conf) {
    return new SortOperator(
        generateNormalizedKeyComputer("SortComputer"),
        generateRecordComparator("SortComparator"),
        conf);
  }

  public GeneratedNormalizedKeyComputer generateNormalizedKeyComputer(String name) {
    String className = generatedClassName(name);
    return new GeneratedNormalizedKeyComputer(className, generateNormalizedKeyComputerCode(className));
  }

  public GeneratedRecordComparator generateRecordComparator(String name) {
    String className = generatedClassName(name);
    return new GeneratedRecordComparator(className, generateRecordComparatorCode(className), fieldGetters);
  }

  private String generatedClassName(String name) {
    String normalizedName = name.replaceAll("[^A-Za-z0-9_$]", "_");
    if (normalizedName.isEmpty() || !Character.isJavaIdentifierStart(normalizedName.charAt(0))) {
      normalizedName = "_" + normalizedName;
    }
    int hash = 31 * Arrays.hashCode(sortIndices) + rowType.asSerializableString().hashCode();
    return normalizedName + "_" + Integer.toUnsignedString(hash);
  }

  private String generateRecordComparatorCode(String className) {
    StringBuilder code = new StringBuilder();
    code.append("public final class ").append(className)
        .append(" implements org.apache.flink.table.runtime.generated.RecordComparator {\n")
        .append("  private final Object[] references;\n")
        .append("  public ").append(className).append("(Object[] references) {\n")
        .append("    this.references = references;\n")
        .append("  }\n")
        .append("  @Override\n")
        .append("  public int compare(org.apache.flink.table.data.RowData row1, ")
        .append("org.apache.flink.table.data.RowData row2) {\n");
    for (int i = 0; i < sortIndices.length; i++) {
      int sortIndex = sortIndices[i];
      code.append("    boolean isNull1_").append(i).append(" = row1.isNullAt(").append(sortIndex).append(");\n")
          .append("    boolean isNull2_").append(i).append(" = row2.isNullAt(").append(sortIndex).append(");\n")
          .append("    if (isNull1_").append(i).append(" || isNull2_").append(i).append(") {\n")
          .append("      if (isNull1_").append(i).append(" && isNull2_").append(i).append(") {\n")
          .append("      } else {\n")
          .append("        return isNull1_").append(i).append(" ? -1 : 1;\n")
          .append("      }\n")
          .append("    } else {\n")
          .append("      int cmp_").append(i).append(" = ").append(compareExpression(i, sortIndex)).append(";\n")
          .append("      if (cmp_").append(i).append(" != 0) {\n")
          .append("        return cmp_").append(i).append(";\n")
          .append("      }\n")
          .append("    }\n");
    }
    code.append("    return 0;\n")
        .append("  }\n")
        .append("  private int compareFallback(org.apache.flink.table.data.RowData row1, ")
        .append("org.apache.flink.table.data.RowData row2, int referenceIndex) {\n")
        .append("    Object value1 = ((org.apache.flink.table.data.RowData.FieldGetter) references[referenceIndex])")
        .append(".getFieldOrNull(row1);\n")
        .append("    Object value2 = ((org.apache.flink.table.data.RowData.FieldGetter) references[referenceIndex])")
        .append(".getFieldOrNull(row2);\n")
        .append("    return compareValues(value1, value2);\n")
        .append("  }\n")
        .append("  private static int compareValues(Object value1, Object value2) {\n")
        .append("    if (value1 == value2) {\n")
        .append("      return 0;\n")
        .append("    }\n")
        .append("    if (value1 == null) {\n")
        .append("      return -1;\n")
        .append("    }\n")
        .append("    if (value2 == null) {\n")
        .append("      return 1;\n")
        .append("    }\n")
        .append("    if (value1 instanceof byte[] && value2 instanceof byte[]) {\n")
        .append("      return compareUnsignedBytes((byte[]) value1, (byte[]) value2);\n")
        .append("    }\n")
        .append("    if (value1 instanceof java.lang.Comparable && value2 instanceof java.lang.Comparable) {\n")
        .append("      return ((java.lang.Comparable) value1).compareTo(value2);\n")
        .append("    }\n")
        .append("    throw new IllegalArgumentException(\"Unsupported sort field value type: \" ")
        .append("+ value1.getClass().getName());\n")
        .append("  }\n")
        .append("  private static int compareUnsignedBytes(byte[] bytes1, byte[] bytes2) {\n")
        .append("    int len = java.lang.Math.min(bytes1.length, bytes2.length);\n")
        .append("    for (int i = 0; i < len; i++) {\n")
        .append("      int result = java.lang.Byte.toUnsignedInt(bytes1[i]) ")
        .append("- java.lang.Byte.toUnsignedInt(bytes2[i]);\n")
        .append("      if (result != 0) {\n")
        .append("        return result;\n")
        .append("      }\n")
        .append("    }\n")
        .append("    return bytes1.length - bytes2.length;\n")
        .append("  }\n")
        .append("}\n");
    return code.toString();
  }

  private String compareExpression(int referenceIndex, int sortIndex) {
    LogicalType logicalType = rowType.getTypeAt(sortIndex);
    switch (logicalType.getTypeRoot()) {
      case BOOLEAN:
        return "java.lang.Boolean.compare(row1.getBoolean(" + sortIndex + "), row2.getBoolean(" + sortIndex + "))";
      case TINYINT:
        return "java.lang.Byte.compare(row1.getByte(" + sortIndex + "), row2.getByte(" + sortIndex + "))";
      case SMALLINT:
        return "java.lang.Short.compare(row1.getShort(" + sortIndex + "), row2.getShort(" + sortIndex + "))";
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case INTERVAL_YEAR_MONTH:
        return "java.lang.Integer.compare(row1.getInt(" + sortIndex + "), row2.getInt(" + sortIndex + "))";
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return "java.lang.Long.compare(row1.getLong(" + sortIndex + "), row2.getLong(" + sortIndex + "))";
      case FLOAT:
        return "java.lang.Float.compare(row1.getFloat(" + sortIndex + "), row2.getFloat(" + sortIndex + "))";
      case DOUBLE:
        return "java.lang.Double.compare(row1.getDouble(" + sortIndex + "), row2.getDouble(" + sortIndex + "))";
      case CHAR:
      case VARCHAR:
        return "row1.getString(" + sortIndex + ").compareTo(row2.getString(" + sortIndex + "))";
      case BINARY:
      case VARBINARY:
        return "compareUnsignedBytes(row1.getBinary(" + sortIndex + "), row2.getBinary(" + sortIndex + "))";
      case DECIMAL:
        DecimalType decimalType = (DecimalType) logicalType;
        return "row1.getDecimal(" + sortIndex + ", " + decimalType.getPrecision() + ", "
            + decimalType.getScale() + ").compareTo(row2.getDecimal(" + sortIndex + ", "
            + decimalType.getPrecision() + ", " + decimalType.getScale() + "))";
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return timestampCompareExpression(sortIndex, ((TimestampType) logicalType).getPrecision());
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return timestampCompareExpression(sortIndex, ((LocalZonedTimestampType) logicalType).getPrecision());
      case TIMESTAMP_WITH_TIME_ZONE:
        return timestampCompareExpression(sortIndex, ((ZonedTimestampType) logicalType).getPrecision());
      default:
        return "compareFallback(row1, row2, " + referenceIndex + ")";
    }
  }

  private String timestampCompareExpression(int sortIndex, int precision) {
    return "row1.getTimestamp(" + sortIndex + ", " + precision + ").compareTo(row2.getTimestamp("
        + sortIndex + ", " + precision + "))";
  }

  private String generateNormalizedKeyComputerCode(String className) {
    return "public final class " + className
        + " implements org.apache.flink.table.runtime.generated.NormalizedKeyComputer {\n"
        + "  public " + className + "(Object[] references) {\n"
        + "  }\n"
        + "  @Override\n"
        + "  public void putKey(org.apache.flink.table.data.RowData rowData, "
        + "org.apache.flink.core.memory.MemorySegment target, int offset) {\n"
        + "    target.put(offset, (byte) 0);\n"
        + "  }\n"
        + "  @Override\n"
        + "  public int compareKey(org.apache.flink.core.memory.MemorySegment memorySegment, int i, "
        + "org.apache.flink.core.memory.MemorySegment target, int offset) {\n"
        + "    return 0;\n"
        + "  }\n"
        + "  @Override\n"
        + "  public void swapKey(org.apache.flink.core.memory.MemorySegment seg1, int index1, "
        + "org.apache.flink.core.memory.MemorySegment seg2, int index2) {\n"
        + "    byte tmp = seg1.get(index1);\n"
        + "    seg1.put(index1, seg2.get(index2));\n"
        + "    seg2.put(index2, tmp);\n"
        + "  }\n"
        + "  @Override\n"
        + "  public int getNumKeyBytes() {\n"
        + "    return 1;\n"
        + "  }\n"
        + "  @Override\n"
        + "  public boolean isKeyFullyDetermines() {\n"
        + "    return false;\n"
        + "  }\n"
        + "  @Override\n"
        + "  public boolean invertKey() {\n"
        + "    return false;\n"
        + "  }\n"
        + "}\n";
  }
}
