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

import org.apache.hudi.common.HoodieJsonPayload;
import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.model.SerializableIndexedRecord;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.storage.StoragePath;

import com.esotericsoftware.kryo.Kryo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

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
    Arrays.stream(new Class<?>[] {
        HoodieAvroRecord.class,
        HoodieAvroIndexedRecord.class,
        HoodieEmptyRecord.class,

        OverwriteWithLatestAvroPayload.class,
        DefaultHoodieRecordPayload.class,
        OverwriteNonDefaultsWithLatestAvroPayload.class,
        RewriteAvroPayload.class,
        EventTimeAvroPayload.class,
        PartialUpdateAvroPayload.class,
        MySqlDebeziumAvroPayload.class,
        PostgresDebeziumAvroPayload.class,
        // TODO need to relocate to hudi-common
        //kryo.register(BootstrapRecordPayload.class);
        AWSDmsAvroPayload.class,
        HoodieAvroPayload.class,
        HoodieJsonPayload.class,
        HoodieMetadataPayload.class,

        HoodieRecordLocation.class,
        HoodieRecordGlobalLocation.class,
        HoodieRecordDelegate.class,
        HoodieInstant.class,
        HoodieActiveTimeline.class,
        HoodieTableConfig.class,
        ConsistencyGuardConfig.class,
        FileSystemRetryConfig.class,
        HoodieMetaserverConfig.class,
        HoodieTimeGeneratorConfig.class,
        Option.class,
        HoodieIndexMetadata.class,
        HashMap.class,
        StoragePath.class,
        HoodieTableMetaClient.class,
        HoodieFileGroupId.class,
        ArrayList.class,
        SerializableIndexedRecord.class
    })
        .forEachOrdered(kryo::register);
  }

}
