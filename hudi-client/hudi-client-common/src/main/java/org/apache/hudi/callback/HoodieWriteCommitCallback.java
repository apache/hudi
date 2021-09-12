/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.callback;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;

/**
 * A callback interface help to call back when a write commit completes successfully.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public interface HoodieWriteCommitCallback {

  /**
   * A callback method the user can implement to provide asynchronous handling of successful write.
   * This method will be called when a write operation is committed successfully.
   *
   * @param callbackMessage Callback msg, which will be sent to external system.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  void call(HoodieWriteCommitCallbackMessage callbackMessage);

}
