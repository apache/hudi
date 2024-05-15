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

package org.apache.hudi.utilities.applied.schema;

import org.apache.avro.Schema;

public class UrsaWrapperSchema {
  public static final String READER_PARQUET_SCHEMA = "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"ursa\",\n"
          + "  \"namespace\" : \"co.applied.ursa\",\n"
          + "  \"fields\" : [ {\n"
          + "    \"name\" : \"run_uuid\",\n"
          + "    \"type\" : [ \"null\", \"string\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"timestamp_ns\",\n"
          + "    \"type\" : [ \"null\", \"long\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"serialized_customer_frame\",\n"
          + "    \"type\" : [ \"null\", \"bytes\" ],\n"
          + "    \"default\" : null\n"
          + "  } ]\n"
          + "}";

  public static final String WRITER_HEADER_SCHEMA = "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"ursa_source\",\n"
          + "  \"namespace\" : \"ursa.source\",\n"
          + "  \"fields\" : [ {\n"
          + "    \"name\" : \"_lake_batch_uuid\",\n"
          + "    \"type\" : [ \"null\", \"string\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"_lake_region\",\n"
          + "    \"type\" : [ \"null\", \"string\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"_lake_bucket\",\n"
          + "    \"type\" : [ \"null\", \"string\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"_lake_source_type\",\n"
          + "    \"type\" : [ \"null\", \"string\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"_lake_batch_key\",\n"
          + "    \"type\" : [ \"null\", \"string\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"_lake_batch_created_timestamp_sec\",\n"
          + "    \"type\" : [ \"null\", \"int\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"_lake_origin_created_timestamp_sec\",\n"
          + "    \"type\" : [ \"null\", \"int\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"_lake_origin_updated_timestamp_sec\",\n"
          + "    \"type\" : [ \"null\", \"int\" ],\n"
          + "    \"default\" : null\n"
          + "  }, {\n"
          + "    \"name\" : \"_lake_file_url\",\n"
          + "    \"type\" : [ \"null\", \"string\" ],\n"
          + "    \"default\" : null\n"
          + "  } ]\n"
          + "}";
  public static final Schema URSA_READER_HEADER_SCHEMA = Schema.parse(READER_PARQUET_SCHEMA, true);
  public static final Schema URSA_WRITER_HEADER_SCHEMA = Schema.parse(WRITER_HEADER_SCHEMA, true);
}
