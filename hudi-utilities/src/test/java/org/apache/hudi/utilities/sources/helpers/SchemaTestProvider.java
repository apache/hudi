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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.utilities.schema.SchemaProvider;

import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link SchemaProvider} for tests.
 */
public class SchemaTestProvider extends SchemaProvider {

  public static AtomicReference<HoodieSchema> schemaToReturn = new AtomicReference<>(HoodieTestDataGenerator.HOODIE_SCHEMA);

  public SchemaTestProvider(TypedProperties props) {
    super(props);
  }

  @Override
  public HoodieSchema getSourceHoodieSchema() {
    return schemaToReturn.get();
  }

}
