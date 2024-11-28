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

package org.apache.hudi.common.util;

import org.apache.avro.Schema;

/**
 * Avro schema for different archived instant read cases.
 */
public abstract class ArchivedInstantReadSchemas {
  public static final Schema TIMELINE_LSM_READ_SCHEMA_WITH_TIME = new Schema.Parser().parse("{\n"
      + "   \"type\":\"record\",\n"
      + "   \"name\":\"HoodieArchivedMetaEntryV2\",\n"
      + "   \"namespace\":\"org.apache.hudi.avro.model\",\n"
      + "   \"fields\":[\n"
      + "      {\n"
      + "         \"name\":\"instantTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"completionTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      }\n"
      + "   ]\n"
      + "}");

  public static final Schema TIMELINE_LSM_READ_SCHEMA_WITH_ACTION = new Schema.Parser().parse("{\n"
      + "   \"type\":\"record\",\n"
      + "   \"name\":\"HoodieArchivedMetaEntryV2\",\n"
      + "   \"namespace\":\"org.apache.hudi.avro.model\",\n"
      + "   \"fields\":[\n"
      + "      {\n"
      + "         \"name\":\"instantTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"completionTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"action\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      }\n"
      + "   ]\n"
      + "}");

  public static final Schema TIMELINE_LSM_READ_SCHEMA_WITH_METADATA = new Schema.Parser().parse("{\n"
      + "   \"type\":\"record\",\n"
      + "   \"name\":\"HoodieArchivedMetaEntryV2\",\n"
      + "   \"namespace\":\"org.apache.hudi.avro.model\",\n"
      + "   \"fields\":[\n"
      + "      {\n"
      + "         \"name\":\"instantTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"completionTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"action\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"metadata\",\n"
      + "         \"type\":[\"null\", \"bytes\"],\n"
      + "         \"default\": null\n"
      + "      }\n"
      + "   ]\n"
      + "}");

  public static final Schema TIMELINE_LSM_READ_SCHEMA_WITH_PLAN = new Schema.Parser().parse("{\n"
      + "   \"type\":\"record\",\n"
      + "   \"name\":\"HoodieArchivedMetaEntryV2\",\n"
      + "   \"namespace\":\"org.apache.hudi.avro.model\",\n"
      + "   \"fields\":[\n"
      + "      {\n"
      + "         \"name\":\"instantTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"completionTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"action\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"plan\",\n"
      + "         \"type\":[\"null\", \"bytes\"],\n"
      + "         \"default\": null\n"
      + "      }\n"
      + "   ]\n"
      + "}");

  public static final Schema TIMELINE_LSM_READ_SCHEMA_AS_FULL = new Schema.Parser().parse("{\n"
      + "   \"type\":\"record\",\n"
      + "   \"name\":\"HoodieArchivedMetaEntryV2\",\n"
      + "   \"namespace\":\"org.apache.hudi.avro.model\",\n"
      + "   \"fields\":[\n"
      + "      {\n"
      + "         \"name\":\"instantTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"completionTime\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"action\",\n"
      + "         \"type\":[\"null\",\"string\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"plan\",\n"
      + "         \"type\":[\"null\", \"bytes\"],\n"
      + "         \"default\": null\n"
      + "      },\n"
      + "      {\n"
      + "         \"name\":\"metadata\",\n"
      + "         \"type\":[\"null\", \"bytes\"],\n"
      + "         \"default\": null\n"
      + "      }\n"
      + "   ]\n"
      + "}");
}
