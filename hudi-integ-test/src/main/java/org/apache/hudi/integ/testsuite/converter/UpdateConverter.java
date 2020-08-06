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

package org.apache.hudi.integ.testsuite.converter;

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.integ.testsuite.generator.LazyRecordGeneratorIterator;
import org.apache.hudi.integ.testsuite.generator.UpdateGeneratorIterator;
import org.apache.spark.api.java.JavaRDD;

/**
 * This converter creates an update {@link GenericRecord} from an existing {@link GenericRecord}.
 */
public class UpdateConverter implements Converter<GenericRecord, GenericRecord> {

  private final String schemaStr;
  // The fields that should not be mutated when converting the insert record to an update record, generally the
  // record_key
  private final List<String> partitionPathFields;
  private final List<String> recordKeyFields;
  private final int minPayloadSize;

  public UpdateConverter(String schemaStr, int minPayloadSize, List<String> partitionPathFields,
      List<String> recordKeyFields) {
    this.schemaStr = schemaStr;
    this.partitionPathFields = partitionPathFields;
    this.recordKeyFields = recordKeyFields;
    this.minPayloadSize = minPayloadSize;
  }

  @Override
  public JavaRDD<GenericRecord> convert(JavaRDD<GenericRecord> inputRDD) {
    return inputRDD.mapPartitions(recordItr -> new LazyRecordGeneratorIterator(new UpdateGeneratorIterator(recordItr,
        schemaStr, partitionPathFields, recordKeyFields, minPayloadSize)));
  }

}
