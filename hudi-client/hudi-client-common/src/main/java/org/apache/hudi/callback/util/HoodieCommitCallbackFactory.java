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

package org.apache.hudi.callback.util;

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteCommitCallbackConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitCallbackException;

/**
 * Factory help to create {@link HoodieWriteCommitCallback}.
 */
public class HoodieCommitCallbackFactory {
  public static HoodieWriteCommitCallback create(HoodieWriteConfig config) {
    String callbackClass = config.getString(HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_NAME);
    if (!StringUtils.isNullOrEmpty(callbackClass)) {
      Object instance = ReflectionUtils.loadClass(callbackClass, config);
      if (!(instance instanceof HoodieWriteCommitCallback)) {
        throw new HoodieCommitCallbackException(callbackClass + " is not a subclass of "
            + HoodieWriteCommitCallback.class.getSimpleName());
      }
      return (HoodieWriteCommitCallback) instance;
    } else {
      throw new HoodieCommitCallbackException(String.format("The value of the config option %s can not be null or "
          + "empty", HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_NAME.key()));
    }
  }

}
