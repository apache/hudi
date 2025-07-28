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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;

import java.io.IOException;

public class HoodiePreCombineAvroBinaryRecordMerger extends HoodieAvroRecordMerger {
  public static final HoodiePreCombineAvroBinaryRecordMerger INSTANCE = new HoodiePreCombineAvroBinaryRecordMerger();

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    // To be fixed.
    // if newer is deleted and default ordering value, return new.
    // if newer is deleted and non default ordering value, compare w/ old and return.
    // if old is deleted, and default ordering value, return new.
    // if not, compare ordering values and return the record that has higher ord value.

    try {
      Option<Pair<HoodieRecord, Schema>> deleteHandlingResult = handleDeletes(older, oldSchema, newer, newSchema, props);
      if (deleteHandlingResult != null) {
        return deleteHandlingResult;
      }

      if (older.getOrderingValue(oldSchema, props).compareTo(newer.getOrderingValue(newSchema, props)) > 0) {
        return Option.of(Pair.of(older, oldSchema));
      } else {
        return Option.of(Pair.of(newer, newSchema));
      }

    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser records ", e);
    }
  }

  @SuppressWarnings("rawtypes, unchecked")
  private Pair<HoodieRecord, Schema> preCombine(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) {
    // To be fixed.
    throw new HoodieIOException("Not implemented");
  }

  protected Option<Pair<HoodieRecord, Schema>> handleDeletes(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    // if newer is delete,
    if (newer.isDelete(newSchema, props)) {
      // Delete record
      return Option.empty();
    }

    if (older.isDelete(oldSchema, props)) {
      // use natural order for delete record
      return Option.of(Pair.of(newer, newSchema));
    }
    return null;
  }
}