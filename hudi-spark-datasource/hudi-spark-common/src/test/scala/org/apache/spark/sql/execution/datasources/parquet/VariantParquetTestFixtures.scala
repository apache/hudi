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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
 * The parquet layouts the shredded-variant guards are tested against, plus the LIST/MAP walking
 * rules production resolves them by. The layouts are shared by [[TestParquetSchemaEvolutionUtils]]
 * here and TestSpark40HoodieParquetReadSupport in hudi-spark4.0.x; the walking rules are used by
 * VariantShreddingTestSupport in hudi-spark. Both of those out-of-module suites pull this module's
 * test-jar. One definition per shape keeps the guards pinned against the same files.
 *
 * Only parquet types are named here, so the object stays usable from every Spark-version module.
 */
object VariantParquetTestFixtures {

  /**
   * The shredded parquet layout: {metadata, value, typed_value}. The group repetition is not part
   * of what the guards check, so one optional shape serves every wrapper below.
   */
  def shreddedVariant(name: String): Type =
    Types.optionalGroup()
      .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
      .addField(Types.optional(PrimitiveTypeName.BINARY).named("value"))
      .addField(Types.optionalGroup()
        .addField(Types.optional(PrimitiveTypeName.INT32).named("a")).named("typed_value"))
      .named(name)

  /** The unshredded twin: {metadata, value}. */
  def unshreddedVariant(name: String): Type =
    Types.optionalGroup()
      .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
      .addField(Types.required(PrimitiveTypeName.BINARY).named("value"))
      .named(name)

  /** Spark's 3-level list of `element`: group -> repeated "list" -> element. */
  def threeLevelList(name: String, element: Type): Type =
    Types.optionalGroup().as(LogicalTypeAnnotation.listType())
      .addField(Types.repeatedGroup().addField(element).named("list"))
      .named(name)

  /**
   * A 2-level list as parquet-avro ("array") and parquet-thrift ("<field>_tuple") write it:
   * the repeated group, named `repeatedName`, is itself the element record.
   */
  def twoLevelList(name: String, repeatedName: String, element: Type): Type =
    Types.optionalGroup().as(LogicalTypeAnnotation.listType())
      .addField(Types.repeatedGroup().addField(element).named(repeatedName))
      .named(name)

  /** A map of string to `value` in the standard key_value layout. */
  def stringKeyMap(name: String, value: Type): Type =
    Types.optionalGroup().as(LogicalTypeAnnotation.mapType())
      .addField(Types.repeatedGroup()
        .addField(Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key"))
        .addField(value)
        .named("key_value"))
      .named(name)

  /**
   * The element of a parquet LIST group, resolved by the same function the read-side guards use,
   * so a test walking a file cannot drift from the rule production applies: both the 3-level
   * layout the Spark writer emits (group -> repeated "list" -> element) and the 2-level one
   * parquet-avro emits (the repeated group, named "array" or "<field>_tuple", IS the element) are
   * covered there. A shape the rule does not recognize is a broken fixture, so it fails here
   * rather than returning the None production uses to stop a walk without failing a read.
   */
  def listElement(list: GroupType): Type =
    ParquetSchemaEvolutionUtils.parquetListElement(list).getOrElse(
      throw new IllegalArgumentException(s"not a parquet LIST layout the read guards resolve:\n$list"))

  /** The value of a parquet MAP group, per ParquetSchemaEvolutionUtils.parquetMapValue. */
  def mapValue(map: GroupType): Type =
    ParquetSchemaEvolutionUtils.parquetMapValue(map).getOrElse(
      throw new IllegalArgumentException(s"not a parquet MAP layout the read guards resolve:\n$map"))
}
