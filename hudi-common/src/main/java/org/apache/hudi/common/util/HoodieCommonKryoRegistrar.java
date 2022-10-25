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

package org.apache.hudi.common.util;

import com.esotericsoftware.kryo.Kryo;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.HoodieJsonPayload;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.metadata.HoodieMetadataPayload;

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
public class HoodieCommonKryoRegistrar {

  public void registerClasses(Kryo kryo) {
    ///////////////////////////////////////////////////////////////////////////
    // NOTE: DO NOT REORDER REGISTRATIONS
    ///////////////////////////////////////////////////////////////////////////

    kryo.register(HoodieAvroRecord.class);
    kryo.register(HoodieAvroIndexedRecord.class);
    kryo.register(HoodieEmptyRecord.class);

    kryo.register(OverwriteWithLatestAvroPayload.class);
    kryo.register(DefaultHoodieRecordPayload.class);
    kryo.register(OverwriteNonDefaultsWithLatestAvroPayload.class);
    kryo.register(RewriteAvroPayload.class);
    kryo.register(EventTimeAvroPayload.class);
    kryo.register(PartialUpdateAvroPayload.class);
    kryo.register(MySqlDebeziumAvroPayload.class);
    kryo.register(PostgresDebeziumAvroPayload.class);
    // TODO need to relocate to hudi-common
    //kryo.register(BootstrapRecordPayload.class);
    kryo.register(AWSDmsAvroPayload.class);
    kryo.register(HoodieAvroPayload.class);
    kryo.register(HoodieJsonPayload.class);
    kryo.register(HoodieMetadataPayload.class);

    kryo.register(HoodieRecordLocation.class);
    kryo.register(HoodieRecordGlobalLocation.class);

    kryo.register(Utf8.class, new SerializationUtils.AvroUtf8Serializer());
  }

}
