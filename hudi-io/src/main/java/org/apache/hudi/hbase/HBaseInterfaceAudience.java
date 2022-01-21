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

// TODO move this to hbase-annotations non-test-jar

/**
 * This class defines constants for different classes of hbase limited private apis
 */
@InterfaceAudience.Public
public final class HBaseInterfaceAudience {

  /**
   * Can't create this class.
   */
  private HBaseInterfaceAudience(){}

  public static final String COPROC = "Coprocesssor";
  public static final String REPLICATION = "Replication";
  public static final String PHOENIX = "Phoenix";
  public static final String SPARK = "Spark";
  public static final String UNITTEST = "Unittest";

  /**
   * Denotes class names that appear in user facing configuration files.
   */
  public static final String CONFIG = "Configuration";

  /**
   * Denotes classes used as tools (Used from cmd line). Usually, the compatibility is required
   * for class name, and arguments.
   */
  public static final String TOOLS = "Tools";

  /**
   * Denotes classes used by hbck tool for fixing inconsistent state of HBase.
   */
  public static final String HBCK = "HBCK";

  /**
   * Denotes classes that can be used to build custom authentication solutions.
   */
  public static final String AUTHENTICATION = "Authentication";
}
