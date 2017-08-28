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

package com.uber.hoodie;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.exception.HoodieException;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Simple key generator, which takes names of fields to be used for recordKey and partitionPath
 * as configs.
 */
public class SimpleKeyGenerator extends KeyGenerator {

    protected final String recordKeyField;

    protected final String partitionPathField;

    public SimpleKeyGenerator(PropertiesConfiguration config) {
        super(config);
        this.recordKeyField = config.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY());
        this.partitionPathField = config.getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY());
    }

    @Override
    public HoodieKey getKey(GenericRecord record) {
        if (recordKeyField == null || partitionPathField == null) {
            throw new HoodieException("Unable to find field names for record key or partition path in cfg");
        }
        return new HoodieKey(DataSourceUtils.getNestedFieldValAsString(record, recordKeyField),
                DataSourceUtils.getNestedFieldValAsString(record, partitionPathField));
    }
}
