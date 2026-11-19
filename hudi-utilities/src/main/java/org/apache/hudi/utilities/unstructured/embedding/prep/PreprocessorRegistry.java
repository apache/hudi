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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Resolves configured file preprocessors by extension; PDF is the shipped document default. */
public class PreprocessorRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorRegistry.class);

  /** The shipped PDF preprocessor, by name: the prep package does not depend on its pdf sub-package. */
  public static final String PDF_PREPROCESSOR_CLASS = "org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFilePreprocessor";
  private static final Map<String, String> DEFAULTS = new HashMap<>();

  static {
    DEFAULTS.put("pdf", PDF_PREPROCESSOR_CLASS);
  }

  private final Map<String, String> classByExtension = new HashMap<>(DEFAULTS);
  private final Map<String, FilePreprocessor> instances = new HashMap<>();

  public PreprocessorRegistry(TypedProperties props) {
    String prefix = EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX;
    String suffix = EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX;
    for (String key : props.stringPropertyNames()) {
      if (EmbeddingTableConfig.isPreprocessorClassKey(key)) {
        String extension = key.substring(prefix.length(), key.length() - suffix.length()).toLowerCase(Locale.ROOT);
        classByExtension.put(extension, ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue()));
      }
    }
  }

  public Option<FilePreprocessor> forExtension(String extension) {
    if (extension == null) {
      return Option.empty();
    }
    String className = classByExtension.get(extension.toLowerCase(Locale.ROOT));
    if (className == null) {
      return Option.empty();
    }
    return Option.of(instances.computeIfAbsent(className, ReflectionUtils::loadClass));
  }

  /**
   * Closes every preprocessor this registry resolved that is {@link AutoCloseable} (one holding per-task resources), once, quietly: a failing close is logged and never hides the outcome it
   * follows. The prep framework calls it when its task's iterator closes.
   */
  public void close() {
    for (FilePreprocessor preprocessor : instances.values()) {
      if (preprocessor instanceof AutoCloseable) {
        try {
          ((AutoCloseable) preprocessor).close();
        } catch (Exception e) {
          LOG.warn("Closing preprocessor {} failed", preprocessor.name(), e);
        }
      }
    }
    instances.clear();
  }

}
