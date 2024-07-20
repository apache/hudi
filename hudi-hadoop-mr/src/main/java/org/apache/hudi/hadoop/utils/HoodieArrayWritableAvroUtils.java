/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.util.collection.Pair;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.function.UnaryOperator;

public class HoodieArrayWritableAvroUtils {

  private static final Cache<String, ObjectInspectorCache>
      OBJECT_INSPECTOR_TABLE_CACHE = Caffeine.newBuilder().maximumSize(1000).build();

  public static ObjectInspectorCache getCacheForTable(String table, Schema tableSchema, JobConf jobConf) {
    ObjectInspectorCache cache = OBJECT_INSPECTOR_TABLE_CACHE.getIfPresent(table);
    if (cache == null) {
      cache = new ObjectInspectorCache(tableSchema, jobConf);
    }
    return cache;
  }

  private static final Cache<Pair<Schema, Schema>, int[]>
      PROJECTION_CACHE = Caffeine.newBuilder().maximumSize(1000).build();

  public static int[] getProjection(Schema from, Schema to) {
    return PROJECTION_CACHE.get(Pair.of(from, to), schemas -> {
      List<Schema.Field> toFields = to.getFields();
      int[] newProjection = new int[toFields.size()];
      for (int i = 0; i < newProjection.length; i++) {
        newProjection[i] = from.getField(toFields.get(i).name()).pos();
      }
      return newProjection;
    });
  }

  /**
   * Projection will keep the size from the "from" schema because it gets recycled
   * and if the size changes the reader will fail
   */
  public static UnaryOperator<ArrayWritable> projectRecord(Schema from, Schema to) {
    int[] projection = getProjection(from, to);
    return arrayWritable -> {
      Writable[] values = new Writable[arrayWritable.get().length];
      for (int i = 0; i < projection.length; i++) {
        values[i] = arrayWritable.get()[projection[i]];
      }
      arrayWritable.set(values);
      return arrayWritable;
    };
  }

  public static int[] getReverseProjection(Schema from, Schema to) {
    return PROJECTION_CACHE.get(Pair.of(from, to), schemas -> {
      List<Schema.Field> fromFields = from.getFields();
      int[] newProjection = new int[fromFields.size()];
      for (int i = 0; i < newProjection.length; i++) {
        newProjection[i] = to.getField(fromFields.get(i).name()).pos();
      }
      return newProjection;
    });
  }

  /**
   * After the reading and merging etc is done, we need to put the records
   * into the positions of the original schema
   */
  public static UnaryOperator<ArrayWritable> reverseProject(Schema from, Schema to) {
    int[] projection = getReverseProjection(from, to);
    return arrayWritable -> {
      Writable[] values = new Writable[to.getFields().size()];
      for (int i = 0; i < projection.length; i++) {
        values[projection[i]] = arrayWritable.get()[i];
      }
      arrayWritable.set(values);
      return arrayWritable;
    };
  }

  public static Object getWritableValue(ArrayWritable arrayWritable, ArrayWritableObjectInspector objectInspector, String name) {
    return objectInspector.getStructFieldData(arrayWritable, objectInspector.getStructFieldRef(name));
  }
}


