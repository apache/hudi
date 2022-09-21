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

import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.util.BuildUtils
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.Types.Field
import org.apache.hudi.internal.schema.utils.{InternalSchemaUtils, SerDeHelper}
import org.apache.hudi.secondary.index.{HoodieSecondaryIndex, ISecondaryIndexReader, SecondaryIndexType}
import org.apache.hudi.secondary.index.filter.{AllRowFilter, AndFilter, EmptyRowFilter, IndexFilter, NotFilter, NotNullFilter, NullFilter, OrFilter, PrefixFilter, RangeFilter, RegexFilter, TermFilter, TermListFilter}
import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

object Spark32PlusHoodieParquetFileFormat {
  /**
   * NOTE: This method is specific to Spark 3.2.0
   */
  def createParquetFilters(args: Any*): ParquetFilters = {
    // NOTE: ParquetFilters ctor args contain Scala enum, therefore we can't look it
    //       up by arg types, and have to instead rely on the number of args based on individual class;
    //       the ctor order is not guaranteed
    val ctor = classOf[ParquetFilters].getConstructors.maxBy(_.getParameterCount)
    ctor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*)
      .asInstanceOf[ParquetFilters]
  }

  /**
   * NOTE: This method is specific to Spark 3.2.0
   */
  def createParquetReadSupport(args: Any*): ParquetReadSupport = {
    // NOTE: ParquetReadSupport ctor args contain Scala enum, therefore we can't look it
    //       up by arg types, and have to instead rely on the number of args based on individual class;
    //       the ctor order is not guaranteed
    val ctor = classOf[ParquetReadSupport].getConstructors.maxBy(_.getParameterCount)
    ctor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*)
      .asInstanceOf[ParquetReadSupport]
  }

  /**
   * NOTE: This method is specific to Spark 3.2.0
   */
  def createVectorizedParquetRecordReader(args: Any*): VectorizedParquetRecordReader = {
    // NOTE: ParquetReadSupport ctor args contain Scala enum, therefore we can't look it
    //       up by arg types, and have to instead rely on the number of args based on individual class;
    //       the ctor order is not guaranteed
    val ctor = classOf[VectorizedParquetRecordReader].getConstructors.maxBy(_.getParameterCount)
    ctor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*)
      .asInstanceOf[VectorizedParquetRecordReader]
  }

  def pruneInternalSchema(internalSchemaStr: String, requiredSchema: StructType): String = {
    val querySchemaOption = SerDeHelper.fromJson(internalSchemaStr)
    if (querySchemaOption.isPresent && requiredSchema.nonEmpty) {
      val prunedSchema = SparkInternalSchemaConverter.convertAndPruneStructTypeToInternalSchema(requiredSchema, querySchemaOption.get())
      SerDeHelper.toJson(prunedSchema)
    } else {
      internalSchemaStr
    }
  }

  def rebuildFilterFromParquet(oldFilter: Filter, fileSchema: InternalSchema, querySchema: InternalSchema): Filter = {
    if (fileSchema == null || querySchema == null) {
      oldFilter
    } else {
      oldFilter match {
        case eq: EqualTo =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eq.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else eq.copy(attribute = newAttribute)
        case eqs: EqualNullSafe =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eqs.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else eqs.copy(attribute = newAttribute)
        case gt: GreaterThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else gt.copy(attribute = newAttribute)
        case gtr: GreaterThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gtr.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else gtr.copy(attribute = newAttribute)
        case lt: LessThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else lt.copy(attribute = newAttribute)
        case lte: LessThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lte.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else lte.copy(attribute = newAttribute)
        case i: In =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(i.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else i.copy(attribute = newAttribute)
        case isn: IsNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else isn.copy(attribute = newAttribute)
        case isnn: IsNotNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isnn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else isnn.copy(attribute = newAttribute)
        case And(left, right) =>
          And(rebuildFilterFromParquet(left, fileSchema, querySchema), rebuildFilterFromParquet(right, fileSchema, querySchema))
        case Or(left, right) =>
          Or(rebuildFilterFromParquet(left, fileSchema, querySchema), rebuildFilterFromParquet(right, fileSchema, querySchema))
        case Not(child) =>
          Not(rebuildFilterFromParquet(child, fileSchema, querySchema))
        case ssw: StringStartsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ssw.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else ssw.copy(attribute = newAttribute)
        case ses: StringEndsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ses.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else ses.copy(attribute = newAttribute)
        case sc: StringContains =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(sc.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else sc.copy(attribute = newAttribute)
        case AlwaysTrue =>
          AlwaysTrue
        case AlwaysFalse =>
          AlwaysFalse
        case _ =>
          AlwaysTrue
      }
    }
  }

  /**
   * Split passed-in filters into index filters and normal filters which will
   * be pushed down as parquet filters
   *
   * @param filters      Spark sql filter
   * @param fields       Hudi fields which converted from parquet file schema
   * @param indexMeta    Secondary index meta data for this table
   * @param validIndex   Indexes which have been built for this base file
   * @param fileName     File name
   * @param indexFolder  Folder to save index data
   * @param indexReaders Index data reader
   * @return Index filters and normal filters
   */
  def splitFilters(
      filters: Seq[Filter],
      fields: Seq[Field],
      indexMeta: Seq[HoodieSecondaryIndex],
      validIndex: Seq[HoodieSecondaryIndex],
      fileName: String,
      indexFolder: String,
      indexReaders: Map[Pair[String, SecondaryIndexType], ISecondaryIndexReader]): (Seq[IndexFilter], Seq[Filter]) = {

    var indexFilters: Seq[IndexFilter] = Seq.empty
    var newFilters: Seq[Filter] = Seq.empty
    filters.foreach(filter => {
      val indexFilter = buildIndexFilter(filter, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
      if (indexFilter.isDefined) {
        indexFilters = indexFilters :+ indexFilter.get
      } else {
        newFilters = newFilters :+ filter
      }
    })

    (indexFilters, newFilters)
  }

  /**
   * Build hoodie index filter from {@code org.apache.spark.sql.sources.Filter}
   * Because of the schema evolution, the field name may be changed, so the
   * passed-in field name must be the real field name,
   *
   * @param filter       Spark sql filter
   * @param fields       Hudi fields which converted from parquet file schema
   * @param indexMeta    Secondary index meta data for this table
   * @param validIndex   Indexes which have been built for this base file
   * @param fileName     File name
   * @param indexFolder  Folder to save index data
   * @param indexReaders Index data reader
   * @return Hoodie index filter
   */
  private def buildIndexFilter(
      filter: Filter,
      fields: Seq[Field],
      indexMeta: Seq[HoodieSecondaryIndex],
      validIndex: Seq[HoodieSecondaryIndex],
      fileName: String,
      indexFolder: String,
      indexReaders: Map[Pair[String, SecondaryIndexType], ISecondaryIndexReader]): Option[IndexFilter] = {
    filter match {
      case eq: EqualTo if canUseIndex(eq.attribute, indexMeta, validIndex) =>
        rebuildHudiField(eq.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new TermFilter(indexReader, field, eq.value)
            })
      case eqs: EqualNullSafe if canUseIndex(eqs.attribute, indexMeta, validIndex) =>
        rebuildHudiField(eqs.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new NullFilter(indexReader, field)
            })
      case gt: GreaterThan if canUseIndex(gt.attribute, indexMeta, validIndex) =>
        rebuildHudiField(gt.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RangeFilter(indexReader, field, gt.value, null, false, false)
            })
      case gtr: GreaterThanOrEqual if canUseIndex(gtr.attribute, indexMeta, validIndex) =>
        rebuildHudiField(gtr.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RangeFilter(indexReader, field, gtr.value, null, true, false)
            })
      case lt: LessThan if canUseIndex(lt.attribute, indexMeta, validIndex) =>
        rebuildHudiField(lt.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RangeFilter(indexReader, field, null, lt.value, false, false)
            })
      case lte: LessThanOrEqual if canUseIndex(lte.attribute, indexMeta, validIndex) =>
        rebuildHudiField(lte.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RangeFilter(indexReader, field, null, lte.value, false, true)
            })
      case i: In if canUseIndex(i.attribute, indexMeta, validIndex) =>
        rebuildHudiField(i.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              val values = i.values.toList.map(_.asInstanceOf[AnyRef]).asJava
              new TermListFilter(indexReader, field, values)
            })
      case isn: IsNull if canUseIndex(isn.attribute, indexMeta, validIndex) =>
        rebuildHudiField(isn.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new NullFilter(indexReader, field)
            })
      case isnn: IsNotNull if canUseIndex(isnn.attribute, indexMeta, validIndex) =>
        rebuildHudiField(isnn.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new NotNullFilter(indexReader, field)
            })
      case And(left, right) =>
        buildIndexFilter(left, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
            .flatMap(left =>
              buildIndexFilter(right, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
                  .map(right => new AndFilter(left, right)))
      case Or(left, right) =>
        buildIndexFilter(left, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
            .flatMap(left =>
              buildIndexFilter(right, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
                  .map(right => new OrFilter(left, right)))
      case Not(child) =>
        buildIndexFilter(child, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
            .map(x => new NotFilter(x))
      case ssw: StringStartsWith if canUseIndex(ssw.attribute, indexMeta, validIndex) =>
        rebuildHudiField(ssw.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new PrefixFilter(indexReader, field, ssw.value)
            })
      case ses: StringEndsWith if canUseIndex(ses.attribute, indexMeta, validIndex) =>
        rebuildHudiField(ses.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RegexFilter(indexReader, field, s"%${ses.value}")
            })
      case sc: StringContains if canUseIndex(sc.attribute, indexMeta, validIndex) =>
        rebuildHudiField(sc.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RegexFilter(indexReader, field, s"%${sc.value}%")
            })
      case AlwaysTrue =>
        Some(new AllRowFilter(null))
      case AlwaysFalse =>
        Some(new EmptyRowFilter(null))
      case _ =>
        None
    }
  }

  /**
   * Check whether the given field can use secondary index with this file
   *
   * @param fieldName  Field name  to be checked
   * @param indexMeta  Secondary index meta data for this table
   * @param validIndex All HoodieSecondaryIndex that have been built index in this file
   * @return true if can use secondary index for this field
   */
  private def canUseIndex(
      fieldName: String,
      indexMeta: Seq[HoodieSecondaryIndex],
      validIndex: Seq[HoodieSecondaryIndex]): Boolean = {
    indexMeta.exists(index =>
      index.getColumns.size() == 1 &&
          index.getColumns.keySet().contains(fieldName) &&
          validIndex.contains(index))
  }

  private def buildIndexReader(
      fieldName: String,
      indexMeta: Seq[HoodieSecondaryIndex],
      fileName: String,
      indexFolder: String,
      indexReaders: Map[Pair[String, SecondaryIndexType], ISecondaryIndexReader]): ISecondaryIndexReader = {
    indexMeta.find(index => index.getColumns.keySet().contains(fieldName))
        .map(index => {
          val indexSavePath = BuildUtils.getIndexSaveDir(indexFolder, index.getIndexType.name(), fileName)
          indexReaders(Pair.of(indexSavePath.toString, index.getIndexType))
        }).get
  }

  /**
   * Get hudi field from given field name
   *
   * @param fieldName Field name in filter
   * @param fields    Hudi fields which converted from parquet file schema
   * @return Hudi field
   */
  private def rebuildHudiField(
      fieldName: String, fields: Seq[Field]): Option[Field] = {
    fields.find(field => field.name().equals(fieldName))
  }
}
