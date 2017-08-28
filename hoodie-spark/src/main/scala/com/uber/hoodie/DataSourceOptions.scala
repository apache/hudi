/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie

import com.uber.hoodie.common.model.HoodieTableType

/**
  * List of options that can be passed to the Hoodie datasource,
  * in addition to the hoodie client configs
  */

/**
  * Options supported for reading hoodie datasets.
  */
object DataSourceReadOptions {
  /**
    * Whether data needs to be read, in
    * incremental mode (new data since an instantTime)
    * (or) Read Optimized mode (obtain latest view, based on columnar data)
    * (or) Real time mode (obtain latest view, based on row & columnar data)
    *
    * Default: READ_OPTIMIZED
    */
  val VIEW_TYPE_OPT_KEY = "hoodie.datasource.view.type"
  val VIEW_TYPE_READ_OPTIMIZED_OPT_VAL = "READ_OPTIMIZED"
  val VIEW_TYPE_INCREMENTAL_OPT_VAL = "INCREMENTAL"
  val VIEW_TYPE_REALTIME_OPT_VAL = "REALTIME"
  val DEFAULT_VIEW_TYPE_OPT_VAL = VIEW_TYPE_READ_OPTIMIZED_OPT_VAL


  /**
    * Instant time to start incrementally pulling data from. The instanttime here need not
    * necessarily correspond to an instant on the timeline. New data written with an
    * `instant_time > BEGIN_INSTANTTIME` are fetched out. For e.g: '20170901080000' will get
    * all new data written after Sep 1, 2017 08:00AM.
    *
    * Default: None (Mandatory in incremental mode)
    */
  val BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime"


  /**
    * Instant time to limit incrementally fetched data to. New data written with an
    * `instant_time <= END_INSTANTTIME` are fetched out.
    *
    * Default: latest instant (i.e fetches all new data since begin instant time)
    *
    */
  val END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime"
}

/**
  * Options supported for writing hoodie datasets.
  */
object DataSourceWriteOptions {
  /**
    * The client operation, that this write should do
    *
    * Default: upsert()
    */
  val OPERATION_OPT_KEY = "hoodie.datasource.write.operation"
  val BULK_INSERT_OPERATION_OPT_VAL = "bulk_insert"
  val INSERT_OPERATION_OPT_VAL = "insert"
  val UPSERT_OPERATION_OPT_VAL = "upsert"
  val DEFAULT_OPERATION_OPT_VAL = UPSERT_OPERATION_OPT_VAL;

  /**
    * The storage type for the underlying data, for this write.
    *
    * Default: COPY_ON_WRITE
    */
  val STORAGE_TYPE_OPT_KEY = "hoodie.datasource.write.storage.type"
  val COW_STORAGE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  val MOR_STORAGE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  val DEFAULT_STORAGE_TYPE_OPT_VAL = COW_STORAGE_TYPE_OPT_VAL

  /**
    * Hive table name, to register the dataset into.
    *
    * Default:  None (mandatory)
    */
  val TABLE_NAME_OPT_KEY = "hoodie.datasource.write.table.name"

  /**
    * Field used in preCombining before actual write. When two records have the same
    * key value, we will pick the one with the largest value for the precombine field,
    * determined by Object.compareTo(..)
    */
  val PRECOMBINE_FIELD_OPT_KEY = "hoodie.datasource.write.precombine.field"
  val DEFAULT_PRECOMBINE_FIELD_OPT_VAL = "ts"


  /**
    * Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
    * This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
    */
  val PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
  val DEFAULT_PAYLOAD_OPT_VAL = classOf[OverwriteWithLatestAvroPayload].getName

  /**
    * Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
    * will be obtained by invoking .toString() on the field value. Nested fields can be specified using
    * the dot notation eg: `a.b.c`
    *
    */
  val RECORDKEY_FIELD_OPT_KEY = "hoodie.datasource.write.recordkey.field"
  val DEFAULT_RECORDKEY_FIELD_OPT_VAL = "uuid"

  /**
    * Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual
    * value ontained by invoking .toString()
    */
  val PARTITIONPATH_FIELD_OPT_KEY = "hoodie.datasource.write.partitionpath.field"
  val DEFAULT_PARTITIONPATH_FIELD_OPT_VAL = "partitionpath"

  /**
    * Key generator class, that implements will extract the key out of incoming record
    *
    */
  val KEYGENERATOR_CLASS_OPT_KEY = "hoodie.datasource.write.keygenerator.class"
  val DEFAULT_KEYGENERATOR_CLASS_OPT_VAL = classOf[SimpleKeyGenerator].getName

  /**
    * Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata.
    * This is useful to store checkpointing information, in a consistent way with the hoodie timeline
    */
  val COMMIT_METADATA_KEYPREFIX_OPT_KEY = "hoodie.datasource.write.commitmeta.key.prefix"
  val DEFAULT_COMMIT_METADATA_KEYPREFIX_OPT_VAL = "_"
}
