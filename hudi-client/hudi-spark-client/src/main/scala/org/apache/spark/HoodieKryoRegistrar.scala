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

package org.apache.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.avro.util.Utf8
import org.apache.hudi.client.bootstrap.BootstrapRecordPayload
import org.apache.hudi.client.model.HoodieInternalRow
import org.apache.hudi.commmon.model.HoodieSparkRecord
import org.apache.hudi.common.HoodieJsonPayload
import org.apache.hudi.common.model.debezium.{MySqlDebeziumAvroPayload, PostgresDebeziumAvroPayload}
import org.apache.hudi.common.model.{AWSDmsAvroPayload, DefaultHoodieRecordPayload, EventTimeAvroPayload, HoodieAvroIndexedRecord, HoodieAvroPayload, HoodieAvroRecord, HoodieEmptyRecord, HoodieRecord, HoodieRecordGlobalLocation, HoodieRecordLocation, OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload, PartialUpdateAvroPayload, RewriteAvroPayload}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.HoodieMetadataPayload
import org.apache.spark.internal.config.Kryo.KRYO_USER_REGISTRATORS
import org.apache.spark.serializer.KryoRegistrator

class HoodieKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    // TODO elaborate (order couldn't change)
    kryo.register(classOf[HoodieWriteConfig])

    kryo.register(classOf[HoodieAvroRecord[_]])
    kryo.register(classOf[HoodieAvroIndexedRecord])
    kryo.register(classOf[HoodieSparkRecord])
    kryo.register(classOf[HoodieEmptyRecord[_]])

    kryo.register(classOf[HoodieInternalRow])

    kryo.register(classOf[HoodieRecordLocation])
    kryo.register(classOf[HoodieRecordGlobalLocation])

    kryo.register(classOf[OverwriteWithLatestAvroPayload])
    kryo.register(classOf[DefaultHoodieRecordPayload])
    kryo.register(classOf[OverwriteNonDefaultsWithLatestAvroPayload])
    kryo.register(classOf[RewriteAvroPayload])
    kryo.register(classOf[EventTimeAvroPayload])
    kryo.register(classOf[PartialUpdateAvroPayload])
    kryo.register(classOf[MySqlDebeziumAvroPayload])
    kryo.register(classOf[PostgresDebeziumAvroPayload])
    kryo.register(classOf[BootstrapRecordPayload])
    kryo.register(classOf[AWSDmsAvroPayload])
    kryo.register(classOf[HoodieAvroPayload])
    kryo.register(classOf[HoodieJsonPayload])
    kryo.register(classOf[HoodieMetadataPayload])

    // TODO add serializer
    kryo.register(classOf[Utf8])
  }
}

object HoodieKryoRegistrar {

  def register(conf: SparkConf): SparkConf = {
    conf.set(KRYO_USER_REGISTRATORS, Seq(classOf[HoodieKryoRegistrar].getName))
  }

}