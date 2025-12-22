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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;

/**
 * The context required to sync one batch of data to hoodie table using StreamSync.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface StreamContext {

  /**
   * The schema provider used for reading data from source and also writing to hoodie table.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  SchemaProvider getSchemaProvider();

  /**
   * An optional stream profile supplying details regarding how the next input batch in StreamSync should be consumed and written.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Option<SourceProfileSupplier> getSourceProfileSupplier();
}
