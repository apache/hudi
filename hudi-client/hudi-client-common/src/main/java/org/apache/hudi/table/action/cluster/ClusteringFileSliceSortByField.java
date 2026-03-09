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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Fields by which file slices can be sorted when creating clustering groups.
 */
@EnumDescription("Fields by which file slices are sorted when creating clustering groups. "
    + "Multiple fields can be specified as a comma-separated list to define sort priority.")
public enum ClusteringFileSliceSortByField {

  @EnumFieldDescription("Sort by the commit/instant time of the file slice's base file in ascending order, "
      + "so that older data files are clustered first (e.g. to reduce stitching lag).")
  INSTANT_TIME,

  @EnumFieldDescription("Sort by the file size of the file slice's base file in descending order, "
      + "so that larger files are clustered first.")
  SIZE
}
