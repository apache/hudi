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

package com.uber.hoodie.utilities.keygen;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.utilities.UtilHelpers;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Arrays;

/**
 * Simple key generator, which takes names of fields to be used for recordKey and partitionPath
 * as configs.
 */
public class SimpleKeyGenerator extends KeyGenerator {

    private final String recordKeyField;

    private final String partitionPathField;

    /**
     * Supported configs
     */
    static class Config {
        private static final String RECORD_KEY_FIELD_PROP = "hoodie.deltastreamer.keygen.simple.recordkey.field";
        private static final String PARTITION_PATH_FIELD_PROP = "hoodie.deltastreamer.keygen.simple.partitionpath.field";
    }

    public SimpleKeyGenerator(PropertiesConfiguration config) {
        super(config);
        UtilHelpers.checkRequiredProperties(config, Arrays.asList(Config.PARTITION_PATH_FIELD_PROP, Config.RECORD_KEY_FIELD_PROP));
        this.recordKeyField = config.getString(Config.RECORD_KEY_FIELD_PROP);
        this.partitionPathField = config.getString(Config.PARTITION_PATH_FIELD_PROP);
    }

    @Override
    public HoodieKey getKey(GenericRecord record) {
        return new HoodieKey(record.get(recordKeyField).toString(), record.get(partitionPathField).toString());
    }
}
