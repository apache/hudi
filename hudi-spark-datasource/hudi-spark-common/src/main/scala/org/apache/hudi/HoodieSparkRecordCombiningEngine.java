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

package org.apache.hudi;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordCombiningEngine;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.Properties;

public class HoodieSparkRecordCombiningEngine implements HoodieRecordCombiningEngine {

  @Override
  public HoodieRecord preCombine(HoodieRecord older, HoodieRecord newer) {
    if (older.getData() == null) {
      // use natural order for delete record
      return older;
    }
    if (older.getOrderingValue().compareTo(newer.getOrderingValue()) > 0) {
      return older;
    } else {
      return newer;
    }
  }

  @Override
  public Option<HoodieRecord> combineAndGetUpdateValue(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
    return Option.of(newer);
  }
}