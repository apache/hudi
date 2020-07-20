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

package org.apache.hudi.utilities.keygen;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.utilities.mongo.SchemaUtils;
import org.bson.types.ObjectId;

/* Key generator which takes the name of the key field to be used for recordKey, computes
 hourly partition values.
 */
public class MongoTimeKeyGenerator extends KeyGenerator {

  private static final String DATE_FORMAT = "yyyy-MM-dd/HH";
  private static final String HIVE_DATE_FORMAT = "'dt='yyyy-MM-dd'/hr='HH";
  private static final String RECORD_KEY = SchemaUtils.ID_FIELD;
  private final boolean hiveStylePartitioning;
  private final SimpleDateFormat dateFormat;

  public MongoTimeKeyGenerator(TypedProperties props) {
    super(props);
    hiveStylePartitioning = props.getBoolean(
        DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));
    dateFormat = new SimpleDateFormat(hiveStylePartitioning ? HIVE_DATE_FORMAT : DATE_FORMAT);
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    String recordKey = DataSourceUtils.getNestedFieldValAsString(record, RECORD_KEY, true);
    if (recordKey == null) {
      throw new HoodieKeyException(SchemaUtils.ID_FIELD + " field value cannot be null");
    }
    Date date = new ObjectId(recordKey).getDate();
    String partitionPath = dateFormat.format(date);
    return new HoodieKey(recordKey, partitionPath);
  }
}
