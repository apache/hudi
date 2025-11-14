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

package org.apache.hudi.schema;

import org.apache.avro.JsonProperties;

/**
 * Hudi wrapper for JSON properties constants, providing compatibility with Avro JsonProperties.
 *
 * <p>This class exposes the same constants as Avro's JsonProperties but through a Hudi-namespaced
 * interface. This allows for seamless migration from Avro JsonProperties to Hudi while maintaining
 * binary compatibility.</p>
 *
 * <p>Usage example:
 * <pre>{@code
 * // Instead of: JsonProperties.NULL_VALUE
 * // Use: HoodieJsonProperties.NULL_VALUE
 *
 * HoodieSchemaField field = HoodieSchemaField.of("name", schema, "doc", HoodieJsonProperties.NULL_VALUE);
 * }</pre></p>
 *
 * @since 1.2.0
 */
public final class HoodieJsonProperties {

  /**
   * Constant representing a null JSON value.
   * This is equivalent to Avro's JsonProperties.NULL_VALUE and maintains binary compatibility.
   */
  public static final Object NULL_VALUE = JsonProperties.NULL_VALUE;

  // Private constructor to prevent instantiation
  private HoodieJsonProperties() {
    throw new UnsupportedOperationException("Utility class cannot be instantiated");
  }
}
