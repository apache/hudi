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

package org.apache.hudi.common.table.marker;

import java.io.Serializable;

/**
 * Stores URLs to timeline server for marker-related operations.
 */
public class MarkerOperation implements Serializable {

  private static final String BASE_URL = "/v1/hoodie/marker";

  public static final String MARKER_DIR_PATH_PARAM = "markerdirpath";
  public static final String MARKER_NAME_PARAM = "markername";
  public static final String MARKER_BASEPATH_PARAM = "basepath";

  // GET requests
  public static final String ALL_MARKERS_URL = String.format("%s/%s", BASE_URL, "all");
  public static final String CREATE_AND_MERGE_MARKERS_URL = String.format("%s/%s", BASE_URL, "create-and-merge");
  public static final String APPEND_MARKERS_URL = String.format("%s/%s", BASE_URL, "append");
  public static final String MARKERS_DIR_EXISTS_URL = String.format("%s/%s", BASE_URL, "dir/exists");

  // POST requests
  public static final String CREATE_MARKER_URL = String.format("%s/%s", BASE_URL, "create");
  public static final String DELETE_MARKER_DIR_URL = String.format("%s/%s", BASE_URL, "dir/delete");
}
