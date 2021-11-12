/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

/**
 * Test sql statements.
 */
public class TestSQL {
  private TestSQL() {
  }

  public static final String INSERT_T1 = "insert into t1 values\n"
      + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
      + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
      + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
      + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
      + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n"
      + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n"
      + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n"
      + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4')";

  public static final String INSERT_SAME_KEY_T1 = "insert into t1 values\n"
      + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:05','par1'),\n"
      + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:04','par1'),\n"
      + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:03','par1'),\n"
      + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
      + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";

  public static final String UPDATE_INSERT_T1 = "insert into t1 values\n"
      + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
      + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
      + "('id3','Julian',54,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
      + "('id4','Fabian',32,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
      + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n"
      + "('id9','Jane',19,TIMESTAMP '1970-01-01 00:00:06','par3'),\n"
      + "('id10','Ella',38,TIMESTAMP '1970-01-01 00:00:07','par4'),\n"
      + "('id11','Phoebe',52,TIMESTAMP '1970-01-01 00:00:08','par4')";
}
