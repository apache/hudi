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

package org.apache.hudi.utilities.schema.postprocessor.add;

import org.apache.hudi.common.config.ConfigProperty;

/**
 * Base configs to describe a primitive type column.
 */
public class BaseSchemaPostProcessorConfig {

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP = ConfigProperty
      .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.name")
      .noDefaultValue()
      .withDocumentation("New column's name");

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_TYPE_PROP = ConfigProperty
      .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.type")
      .noDefaultValue()
      .withDocumentation("New column's type");

  public static final ConfigProperty<Boolean> SCHEMA_POST_PROCESSOR_ADD_COLUMN_NULLABLE_PROP = ConfigProperty
      .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.nullable")
      .defaultValue(true)
      .withDocumentation("New column's nullable");

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_DEFAULT_PROP = ConfigProperty
      .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.default")
      .noDefaultValue()
      .withDocumentation("New column's default value");

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_DOC_PROP = ConfigProperty
      .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.doc")
      .noDefaultValue()
      .withDocumentation("Docs about new column");

}
