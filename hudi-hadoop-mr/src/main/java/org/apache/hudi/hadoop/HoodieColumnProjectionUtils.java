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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  public static final String READ_NESTED_COLUMN_PATH_CONF_STR =
      "hive.io.file.readNestedColumn.paths";
  public static final String READ_ALL_COLUMNS = "hive.io.file.read.all.columns";
  public static final String READ_COLUMN_NAMES_CONF_STR = "hive.io.file.readcolumn.names";
  private static final String READ_COLUMN_IDS_CONF_STR_DEFAULT = "";
  private static final String READ_COLUMN_NAMES_CONF_STR_DEFAULT = "";
  private static final String READ_NESTED_COLUMN_PATH_CONF_STR_DEFAULT = "";
  private static final boolean READ_ALL_COLUMNS_DEFAULT = true;

  private static final String COMMA = ",";

  /** Special Column Names added during Parquet Projection. **/
  public static final String PARQUET_BLOCK_OFFSET_COL_NAME = "BLOCK__OFFSET__INSIDE__FILE";
  public static final String PARQUET_INPUT_FILE_NAME = "INPUT__FILE__NAME";
  public static final String PARQUET_ROW_ID = "ROW__ID";

  public static final List<String> PARQUET_SPECIAL_COLUMN_NAMES =  CollectionUtils
      .createImmutableList(PARQUET_BLOCK_OFFSET_COL_NAME, PARQUET_INPUT_FILE_NAME,
          PARQUET_ROW_ID);

  /**
   * Sets the <em>READ_ALL_COLUMNS</em> flag and removes any previously
   * set column ids.
   */
  public static void setReadAllColumns(Configuration conf) {
    conf.setBoolean(READ_ALL_COLUMNS, true);
    setReadColumnIDConf(conf, READ_COLUMN_IDS_CONF_STR_DEFAULT);
    setReadColumnNamesConf(conf, READ_COLUMN_NAMES_CONF_STR_DEFAULT);
  }

  /**
   * Returns the <em>READ_ALL_COLUMNS</em> columns flag.
   */
  public static boolean isReadAllColumns(Configuration conf) {
    return conf.getBoolean(READ_ALL_COLUMNS, READ_ALL_COLUMNS_DEFAULT);
  }

  /**
   * Sets the <em>READ_ALL_COLUMNS</em> flag to false and overwrites column ids
   * with the provided list.
   */
  public static void setReadColumns(Configuration conf, List<Integer> ids, List<String> names) {
    setReadColumnIDConf(conf, READ_COLUMN_IDS_CONF_STR_DEFAULT);
    setReadColumnNamesConf(conf, READ_COLUMN_NAMES_CONF_STR_DEFAULT);
    appendReadColumns(conf, ids);
    appendReadColumnNames(conf, names);
  }

  /**
   * Appends read columns' ids (start from zero). Once a column
   * is included in the list, a underlying record reader of a columnar file format
   * (e.g. RCFile and ORC) can know what columns are needed.
   */
  public static void appendReadColumns(Configuration conf, List<Integer> ids) {
    String id = toReadColumnIDString(ids);
    String old = conf.get(READ_COLUMN_IDS_CONF_STR, null);
    String newConfStr = id;
    if (old != null && !old.isEmpty()) {
      newConfStr = newConfStr + StringUtils.COMMA_STR + old;
    }
    setReadColumnIDConf(conf, newConfStr);
    // Set READ_ALL_COLUMNS to false
    conf.setBoolean(READ_ALL_COLUMNS, false);
  }

  /**
   * Appends read nested column's paths. Once a read nested column path
   * is included in the list, a underlying record reader of a columnar file format
   * (e.g. Parquet and ORC) can know what columns are needed.
   */
  public static void appendNestedColumnPaths(
      Configuration conf,
      List<String> paths) {
    if (paths == null || paths.isEmpty()) {
      return;
    }
    String pathsStr = StringUtils.join(StringUtils.COMMA_STR,
        paths.toArray(new String[paths.size()]));
    String old = conf.get(READ_NESTED_COLUMN_PATH_CONF_STR, null);
    String newConfStr = pathsStr;
    if (old != null && !old.isEmpty()) {
      newConfStr = newConfStr + StringUtils.COMMA_STR + old;
    }
    setReadNestedColumnPathConf(conf, newConfStr);
  }


  /**
   * This method appends read column information to configuration to use for PPD. It is
   * currently called with information from TSOP. Names come from TSOP input RowSchema, and
   * IDs are the indexes inside the schema (which PPD assumes correspond to indexes inside the
   * files to PPD in; something that would be invalid in many cases of schema evolution).
   * @param conf Config to set values to.
   * @param ids Column ids.
   * @param names Column names.
   */
  public static void appendReadColumns(
      Configuration conf, List<Integer> ids, List<String> names, List<String> groupPaths) {
    if (ids.size() != names.size()) {
      LOG.warn("Read column counts do not match: "
          + ids.size() + " ids, " + names.size() + " names");
    }
    appendReadColumns(conf, ids);
    appendReadColumnNames(conf, names);
    appendNestedColumnPaths(conf, groupPaths);
  }

  public static void appendReadColumns(
      StringBuilder readColumnsBuffer, StringBuilder readColumnNamesBuffer, List<Integer> ids,
      List<String> names) {
    String preppedIdStr = ids.stream().map(x -> String.valueOf(x)).collect(Collectors.joining(COMMA));
    String preppedNamesStr = names.stream().collect(Collectors.joining(COMMA));
    if (readColumnsBuffer.length() > 0) {
      readColumnsBuffer.append(COMMA);
    }
    readColumnsBuffer.append(preppedIdStr);
    if (readColumnNamesBuffer.length() > 0) {
      readColumnNamesBuffer.append(COMMA);
    }
    readColumnNamesBuffer.append(preppedNamesStr);
  }

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

  public static Set<String> getNestedColumnPaths(Configuration conf) {
    String skips =
        conf.get(READ_NESTED_COLUMN_PATH_CONF_STR, READ_NESTED_COLUMN_PATH_CONF_STR_DEFAULT);
    return new HashSet<>(Arrays.asList(StringUtils.split(skips)));
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

  public static void setIOColumnNameAndTypes(Configuration conf, List<Pair<String,String>> colNamesAndTypes) {
    String colNames = colNamesAndTypes.stream().map(e -> e.getKey()).collect(Collectors.joining(","));
    String colTypes = colNamesAndTypes.stream().map(e -> e.getValue()).collect(Collectors.joining(","));
    conf.set(IOConstants.COLUMNS, colNames);
    conf.set(IOConstants.COLUMNS_TYPES, colTypes);
  }

  private static void setReadColumnIDConf(Configuration conf, String id) {
    if (id.trim().isEmpty()) {
      conf.set(READ_COLUMN_IDS_CONF_STR, READ_COLUMN_IDS_CONF_STR_DEFAULT);
    } else {
      conf.set(READ_COLUMN_IDS_CONF_STR, id);
    }
  }

  private static void setReadColumnNamesConf(Configuration conf, String id) {
    if (id.trim().isEmpty()) {
      conf.set(READ_COLUMN_NAMES_CONF_STR, READ_COLUMN_NAMES_CONF_STR_DEFAULT);
    } else {
      conf.set(READ_COLUMN_NAMES_CONF_STR, id);
    }
  }

  private static void setReadNestedColumnPathConf(
      Configuration conf,
      String nestedColumnPaths) {
    nestedColumnPaths = nestedColumnPaths.toLowerCase();
    if (nestedColumnPaths.trim().isEmpty()) {
      conf.set(READ_NESTED_COLUMN_PATH_CONF_STR, READ_NESTED_COLUMN_PATH_CONF_STR_DEFAULT);
    } else {
      conf.set(READ_NESTED_COLUMN_PATH_CONF_STR, nestedColumnPaths);
    }
  }

  private static void appendReadColumnNames(Configuration conf, List<String> cols) {
    String old = conf.get(READ_COLUMN_NAMES_CONF_STR, "");
    StringBuilder result = new StringBuilder(old);
    boolean first = old.isEmpty();
    for (String col: cols) {
      if (first) {
        first = false;
      } else {
        result.append(',');
      }
      result.append(col);
    }
    conf.set(READ_COLUMN_NAMES_CONF_STR, result.toString());
  }

  private static String toReadColumnIDString(List<Integer> ids) {
    String id = "";
    for (int i = 0; i < ids.size(); i++) {
      if (i == 0) {
        id = id + ids.get(i);
      } else {
        id = id + StringUtils.COMMA_STR + ids.get(i);
      }
    }
    return id;
  }

}