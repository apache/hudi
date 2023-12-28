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

package org.apache.hudi.common.table.marker;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Marker type indicating how markers are stored in the file system.
 */
@EnumDescription("Marker type indicating how markers are stored in the file system, used for "
    + "identifying the files written and cleaning up files not committed which should be deleted.")
public enum MarkerType {

  @EnumFieldDescription("Individual marker file corresponding to each data file is directly created by the writer.")
  DIRECT,

  @EnumFieldDescription("Marker operations are all handled at the timeline service which serves as a proxy. "
    + "New marker entries are batch processed and stored in a limited number of underlying files for efficiency. "
    + "If HDFS is used or timeline server is disabled, DIRECT markers are used as fallback even if this is configured. "
    + "This configuration does not take effect for Spark structured streaming; DIRECT markers are always used.")
  TIMELINE_SERVER_BASED
}
