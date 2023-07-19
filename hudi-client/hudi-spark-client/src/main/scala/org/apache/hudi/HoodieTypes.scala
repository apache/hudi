package org.apache.hudi

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.spark.sql.types.StructType

case class HoodieTableSchema(structTypeSchema: StructType, avroSchemaStr: String, internalSchema: Option[InternalSchema] = None)

case class HoodieTableState(tablePath: String,
                            latestCommitTimestamp: Option[String],
                            recordKeyField: String,
                            preCombineFieldOpt: Option[String],
                            usesVirtualKeys: Boolean,
                            recordPayloadClassName: String,
                            metadataConfig: HoodieMetadataConfig,
                            recordMergerImpls: List[String],
                            recordMergerStrategy: String)