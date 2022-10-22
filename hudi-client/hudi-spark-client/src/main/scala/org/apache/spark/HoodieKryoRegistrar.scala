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
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecord, HoodieRecordGlobalLocation, HoodieRecordLocation, OverwriteWithLatestAvroPayload}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.HoodieMetadataPayload
import org.apache.spark.internal.config.Kryo.KRYO_USER_REGISTRATORS
import org.apache.spark.serializer.KryoRegistrator

class HoodieKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    // TODO elaborate (order couldn't change)
    kryo.register(classOf[HoodieWriteConfig])

    // TODO add serializer
    kryo.register(classOf[Utf8])

    kryo.register(classOf[HoodieRecordLocation])
    kryo.register(classOf[HoodieRecordGlobalLocation])

    kryo.register(classOf[OverwriteWithLatestAvroPayload])
    kryo.register(classOf[DefaultHoodieRecordPayload])
    kryo.register(classOf[HoodieMetadataPayload])
  }
}

object HoodieKryoRegistrar {

  def register(conf: SparkConf): SparkConf = {
    conf.set(KRYO_USER_REGISTRATORS, Seq(classOf[HoodieKryoRegistrar].getName))
  }

}