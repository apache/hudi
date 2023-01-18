/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.keygen.factory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.ComplexSparkRecordKeyGenerator;
import org.apache.hudi.keygen.SimpleSparkRecordKeyGenerator;
import org.apache.hudi.keygen.SparkRecordKeyGenerator;
import org.apache.hudi.keygen.SparkRowAccessor;

import java.util.List;

public class SparkRecordKeyGeneratorFactory {

  public static SparkRecordKeyGenerator getSparkRecordKeyGenerator(TypedProperties props, SparkRowAccessor rowAccessor,
                                                                   List<String> recordKeyFields) {
    /*if (props.getBoolean(HoodieWriteConfig.AUTO_GENERATE_RECORD_KEYS.key(), HoodieWriteConfig.AUTO_GENERATE_RECORD_KEYS.defaultValue())) {
      return new AutoSparkRecordKeyGenerator(props);
    }*/

    if (recordKeyFields.size() == 1) {
      return new SimpleSparkRecordKeyGenerator(rowAccessor);
    } else {
      return new ComplexSparkRecordKeyGenerator(recordKeyFields, rowAccessor);
    }
  }

}