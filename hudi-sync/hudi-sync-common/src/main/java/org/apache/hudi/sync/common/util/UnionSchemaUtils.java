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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;

import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for syncing unions of multiple non-null types to the catalog.
 */
public class UnionSchemaUtils {

  private UnionSchemaUtils() {
  }

  /**
   * Returns the fields of the struct that a union of multiple non-null types is synced as.
   *
   * <p>The parquet writer stores such a union as a group of optional "member&lt;N&gt;" fields, one per union
   * branch in declaration order, so each branch becomes a nullable "member&lt;N&gt;" field.
   *
   * @param unionSchema a union without null branches, as returned by {@link HoodieSchema#getNonNullType()}
   */
  public static List<HoodieSchemaField> getUnionMemberFields(HoodieSchema unionSchema) {
    List<HoodieSchema> memberSchemas = unionSchema.getTypes();
    List<HoodieSchemaField> memberFields = new ArrayList<>(memberSchemas.size());
    for (HoodieSchema memberSchema : memberSchemas) {
      memberFields.add(HoodieSchemaField.of("member" + memberFields.size(), HoodieSchema.createNullable(memberSchema)));
    }
    return memberFields;
  }
}
