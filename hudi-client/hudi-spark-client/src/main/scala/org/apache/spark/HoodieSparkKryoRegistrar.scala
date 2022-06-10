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
import org.apache.hudi.client.model.HoodieInternalRow
import org.apache.hudi.common.model.HoodieSparkRecord
import org.apache.hudi.common.util.HoodieCommonKryoRegistrar
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.serializer.KryoRegistrator

/**
 * NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
 *
 * This class is responsible for registering Hudi specific components that are often
 * serialized by Kryo (for ex, during Spark's Shuffling operations) to make sure Kryo
 * doesn't need to serialize their full class-names (for every object) which will quickly
 * add up to considerable amount of overhead.
 *
 * Please note of the following:
 * <ol>
 *   <li>Ordering of the registration COULD NOT change as it's directly impacting
 *   associated class ids (on the Kryo side)</li>
 *   <li>This class might be loaded up using reflection and as such should not be relocated
 *   or renamed (w/o correspondingly updating such usages)</li>
 * </ol>
 */
class HoodieSparkKryoRegistrar extends HoodieCommonKryoRegistrar with KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    ///////////////////////////////////////////////////////////////////////////
    // NOTE: DO NOT REORDER REGISTRATIONS
    ///////////////////////////////////////////////////////////////////////////
    super[HoodieCommonKryoRegistrar].registerClasses(kryo)

    kryo.register(classOf[HoodieWriteConfig])

    kryo.register(classOf[HoodieSparkRecord])
    kryo.register(classOf[HoodieInternalRow])
  }
}

object HoodieSparkKryoRegistrar {

  // NOTE: We're copying definition of the config introduced in Spark 3.0
  //       (to stay compatible w/ Spark 2.4)
  private val KRYO_USER_REGISTRATORS = "spark.kryo.registrator"

  def register(conf: SparkConf): SparkConf = {
    conf.set(KRYO_USER_REGISTRATORS, Seq(classOf[HoodieSparkKryoRegistrar].getName).mkString(","))
  }

}