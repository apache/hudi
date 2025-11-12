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

package org.apache.hudi.storage;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;

import java.io.Serializable;

/**
 * Filter for {@link StoragePath}
 * The APIs are mainly based on {@code org.apache.hadoop.fs.PathFilter} class.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface StoragePathFilter extends Serializable {
  /**
   * Tests whether the specified path should be included in a path list.
   *
   * @param path the path to be tested.
   * @return {@code true} if and only if <code>path</code> should be included.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  boolean accept(StoragePath path);
}
