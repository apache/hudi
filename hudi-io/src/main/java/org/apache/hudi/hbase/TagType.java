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

package org.apache.hudi.hbase;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class TagType {
  // Please declare new Tag Types here to avoid step on pre-existing tag types.
  public static final byte ACL_TAG_TYPE = (byte) 1;
  public static final byte VISIBILITY_TAG_TYPE = (byte) 2;
  // public static final byte LOG_REPLAY_TAG_TYPE = (byte) 3; // deprecated
  public static final byte VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE = (byte)4;

  // mob tags
  public static final byte MOB_REFERENCE_TAG_TYPE = (byte) 5;
  public static final byte MOB_TABLE_NAME_TAG_TYPE = (byte) 6;

  // String based tag type used in replication
  public static final byte STRING_VIS_TAG_TYPE = (byte) 7;
  public static final byte TTL_TAG_TYPE = (byte)8;
}
