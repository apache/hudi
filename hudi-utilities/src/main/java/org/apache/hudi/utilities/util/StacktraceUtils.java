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

package org.apache.hudi.utilities.util;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;
import org.apache.hudi.utilities.transform.Transformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StacktraceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(StacktraceUtils.class);

  public static Class<?> getTransformerClassFromStackTrace(StackTraceElement[] stackTraceElements) {
    for (StackTraceElement element : stackTraceElements) {
      Class<?> clazz;
      try {
        clazz = Class.forName(element.getClassName());
      } catch (ClassNotFoundException e) {
        LOG.error("Unable to find class when analyzing stacktrace", e);
        return null;
      }
      while (clazz != null) {
        if (Transformer.class.isAssignableFrom(clazz)) {
          return clazz;
        }
        clazz = clazz.getEnclosingClass();
      }
    }
    return null;
  }

  public static boolean isSchemaCompatibilityIssue(StackTraceElement[] stackTraceElements) {
    for (StackTraceElement element : stackTraceElements) {
      // Case where we translate into Avro
      if (element.getFileName() == null) {
        continue;
      }
      if (element.getFileName().startsWith(AvroConvertor.class.getSimpleName()) || element.getFileName().startsWith(HoodieAvroUtils.class.getSimpleName())) {
        return true;
      }
      // Case where we translate rows back into Avro format
      if (element.getFileName().startsWith(HoodieSparkUtils.class.getSimpleName()) && (element.getMethodName() != null && element.getMethodName().contains("createRdd"))) {
        return true;
      }
    }
    return false;
  }
}
