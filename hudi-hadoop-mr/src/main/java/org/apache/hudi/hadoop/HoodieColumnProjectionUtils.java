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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME;

/**
 * Utility functions copied from Hive ColumnProjectionUtils.java.
 * Needed to copy as we see NoSuchMethod errors when directly using these APIs with/without Spark.
 * Some of these methods are not available across hive versions.
 */
public class HoodieColumnProjectionUtils {
  public static final Logger LOG = LoggerFactory.getLogger(ColumnProjectionUtils.class);

  public static final String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";
  /**
   * the nested column path is the string from the root to the leaf
   * e.g.
   * c:struct_of (a:string,b:string).
   * the column a's path is c.a and b's path is c.b
   */
  public static final String READ_COLUMN_NAMES_CONF_STR = "hive.io.file.readcolumn.names";
  private static final String READ_COLUMN_IDS_CONF_STR_DEFAULT = "";
  private static final String READ_COLUMN_NAMES_CONF_STR_DEFAULT = "";

  /**
   * Returns an array of column ids(start from zero) which is set in the given
   * parameter <tt>conf</tt>.
   */
  public static List<Integer> getReadColumnIDs(Configuration conf) {
    String skips = conf.get(READ_COLUMN_IDS_CONF_STR, READ_COLUMN_IDS_CONF_STR_DEFAULT);
    String[] list = StringUtils.split(skips);
    List<Integer> result = new ArrayList<Integer>(list.length);
    for (String element : list) {
      // it may contain duplicates, remove duplicates
      Integer toAdd = Integer.parseInt(element);
      if (!result.contains(toAdd)) {
        result.add(toAdd);
      }
      // NOTE: some code uses this list to correlate with column names, and yet these lists may
      //       contain duplicates, which this call will remove and the other won't. As far as I can
      //       tell, no code will actually use these two methods together; all is good if the code
      //       gets the ID list without relying on this method. Or maybe it just works by magic.
    }
    return result;
  }

  public static String[] getReadColumnNames(Configuration conf) {
    String colNames = conf.get(READ_COLUMN_NAMES_CONF_STR, READ_COLUMN_NAMES_CONF_STR_DEFAULT);
    if (colNames != null && !colNames.isEmpty()) {
      return colNames.split(",");
    }
    return new String[] {};
  }

  public static List<String> getIOColumns(Configuration conf) {
    String colNames = conf.get(IOConstants.COLUMNS, "");
    if (colNames != null && !colNames.isEmpty()) {
      return Arrays.asList(colNames.split(","));
    }
    return new ArrayList<>();
  }

  public static List<String> getIOColumnTypes(Configuration conf) {
    String colTypes = conf.get(IOConstants.COLUMNS_TYPES, "");
    if (colTypes != null && !colTypes.isEmpty()) {
      return TypeInfoUtils.getTypeInfosFromTypeString(colTypes).stream()
          .map(t -> t.getTypeName()).collect(Collectors.toList());
    }
    return new ArrayList<>();
  }

  public static List<Pair<String,String>> getIOColumnNameAndTypes(Configuration conf) {
    List<String> names = getIOColumns(conf);
    List<String> types = getIOColumnTypes(conf);
    ValidationUtils.checkArgument(names.size() == types.size());
    return IntStream.range(0, names.size()).mapToObj(idx -> Pair.of(names.get(idx), types.get(idx)))
        .collect(Collectors.toList());
  }

  /**
   * If schema contains timestamp columns, this method is used for compatibility when there is no timestamp fields.
   *
   * <p>We expect to use parquet-avro reader {@link org.apache.hudi.hadoop.avro.HoodieAvroParquetReader} to read
   * timestamp column when read columns contain timestamp type.
   */
  public static boolean supportTimestamp(Configuration conf) {
    List<String> readCols = Arrays.asList(getReadColumnNames(conf));
    if (readCols.isEmpty()) {
      return false;
    }

    String colTypes = conf.get(IOConstants.COLUMNS_TYPES, "");
    if (colTypes == null || colTypes.isEmpty()) {
      return false;
    }

    ArrayList<TypeInfo> types = TypeInfoUtils.getTypeInfosFromTypeString(colTypes);
    List<String> names = getIOColumns(conf);
    return IntStream.range(0, names.size()).filter(i -> readCols.contains(names.get(i)))
        .anyMatch(i -> typeContainsTimestamp(types.get(i)));
  }

  public static boolean typeContainsTimestamp(TypeInfo type) {
    Category category = type.getCategory();

    switch (category) {
      case PRIMITIVE:
        return type.getTypeName().equals(TIMESTAMP_TYPE_NAME);
      case LIST:
        ListTypeInfo listTypeInfo = (ListTypeInfo) type;
        return typeContainsTimestamp(listTypeInfo.getListElementTypeInfo());
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
        return typeContainsTimestamp(mapTypeInfo.getMapKeyTypeInfo())
            || typeContainsTimestamp(mapTypeInfo.getMapValueTypeInfo());
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) type;
        return structTypeInfo.getAllStructFieldTypeInfos().stream()
            .anyMatch(HoodieColumnProjectionUtils::typeContainsTimestamp);
      case UNION:
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) type;
        return unionTypeInfo.getAllUnionObjectTypeInfos().stream()
            .anyMatch(HoodieColumnProjectionUtils::typeContainsTimestamp);
      default:
        return false;
    }
  }
}
