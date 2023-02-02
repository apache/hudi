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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

public class TerminationStrategyUtils {

  /**
   * Create a PostWriteTerminationStrategy class via reflection,
   * <br>
   * if the class name of PostWriteTerminationStrategy is configured through the {@link HoodieDeltaStreamer.Config#postWriteTerminationStrategyClass}.
   */
  public static Option<PostWriteTerminationStrategy> createPostWriteTerminationStrategy(TypedProperties properties, String postWriteTerminationStrategyClass)
      throws HoodieException {
    try {
      return StringUtils.isNullOrEmpty(postWriteTerminationStrategyClass)
          ? Option.empty() :
          Option.of((PostWriteTerminationStrategy) ReflectionUtils.loadClass(postWriteTerminationStrategyClass, properties));
    } catch (Throwable e) {
      throw new HoodieException("Could not create PostWritTerminationStrategy class " + postWriteTerminationStrategyClass, e);
    }
  }
}
