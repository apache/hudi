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

package org.apache.hudi;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.OrderingValueJavaTypeConverter;
import org.apache.hudi.common.util.collection.ArrayComparable;

import java.util.Properties;

/** Converts empty-record ordering values without retaining a Spark record context. */
public enum SparkOrderingValueJavaTypeConverter implements OrderingValueJavaTypeConverter {
  INSTANCE;

  @Override
  public Comparable<?> convert(Comparable<?> orderingValue, HoodieSchema schema, Properties props, String[] orderingFields) {
    if (orderingValue instanceof ArrayComparable) {
      return ((ArrayComparable) orderingValue).apply(value -> (Comparable) BaseSparkInternalRecordContext.sparkTypeToJavaType(value));
    }
    return (Comparable<?>) BaseSparkInternalRecordContext.sparkTypeToJavaType(orderingValue);
  }
}
