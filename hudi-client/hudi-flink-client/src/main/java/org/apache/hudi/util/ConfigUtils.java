/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;

public class ConfigUtils {

  public static final String NO_PRE_COMBINE = "no_precombine";

  /**
   * todo add doc
   * @param props
   * @return
   */
  public static String getPreCombineField(TypedProperties props) {
    String preCombineField = props.getString("precombine.field", null);
    if (preCombineField != null && !preCombineField.equalsIgnoreCase(NO_PRE_COMBINE)) {
      return preCombineField;
    }
    preCombineField = props.getString("write.precombine.field", null);
    if (preCombineField != null && !preCombineField.equalsIgnoreCase(NO_PRE_COMBINE)) {
      return preCombineField;
    }
    preCombineField = props.getString(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), null);
    if (preCombineField != null && !preCombineField.equalsIgnoreCase(NO_PRE_COMBINE)) {
      return preCombineField;
    } else {
      return null;
    }
  }
}
