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

  public static final String COMPLEX_TYPE_INSERT_T1 = "insert into t1 values\n"
      + "(1, array['abc1', 'def1'], map['abc1', 1, 'def1', 3], row(1, 'abc1')),\n"
      + "(2, array['abc2', 'def2'], map['abc2', 1, 'def2', 3], row(2, 'abc2')),\n"
      + "(3, array['abc3', 'def3'], map['abc3', 1, 'def3', 3], row(3, 'abc3'))";

  public static final String COMPLEX_NESTED_ROW_TYPE_INSERT_T1 = "insert into t1 values\n"
      + "(1, array['abc1', 'def1'], array[1, 1], map['abc1', 1, 'def1', 3], row(array['abc1', 'def1'], row(1, 'abc1'))),\n"
      + "(2, array['abc2', 'def2'], array[2, 2], map['abc2', 1, 'def2', 3], row(array['abc2', 'def2'], row(2, 'abc2'))),\n"
      + "(3, array['abc3', 'def3'], array[3, 3], map['abc3', 1, 'def3', 3], row(array['abc3', 'def3'], row(3, 'abc3')))";

  public static final String ARRAY_MAP_OF_ROW_TYPE_INSERT_T1 = "insert into t1 values\n"
          + "(1, array[row('abc11', 11), row('abc12', 12), row('abc13', 13)], map['abc11', row(11, 'def11'), 'abc12', row(12, 'def12'), 'abc13', row(13, 'def13')]),\n"
          + "(2, array[row('abc21', 21), row('abc22', 22), row('abc23', 23)], map['abc21', row(21, 'def21'), 'abc22', row(22, 'def22'), 'abc23', row(23, 'def23')]),\n"
          + "(3, array[row('abc31', 31), row('abc32', 32), row('abc33', 33)], map['abc31', row(31, 'def31'), 'abc32', row(32, 'def32'), 'abc33', row(33, 'def33')])";

  public static final String NULL_CHILD_COLUMNS_ROW_TYPE_INSERT_T1 = "insert into t1 values\n"
      + "(1, row(cast(null as int), 'abc1')),\n"
      + "(2, row(2, cast(null as varchar))),\n"
      + "(3, row(cast(null as int), cast(null as varchar)))";

  public static final String INSERT_DATE_PARTITION_T1 = "insert into t1 values\n"
      + "('id1','Danny',23,DATE '1970-01-01'),\n"
      + "('id2','Stephen',33,DATE '1970-01-01'),\n"
      + "('id3','Julian',53,DATE '1970-01-01'),\n"
      + "('id4','Fabian',31,DATE '1970-01-01'),\n"
      + "('id5','Sophia',18,DATE '1970-01-01'),\n"
      + "('id6','Emma',20,DATE '1970-01-01'),\n"
      + "('id7','Bob',44,DATE '1970-01-01'),\n"
      + "('id8','Han',56,DATE '1970-01-01')";

  public static String insertT1WithSQLHint(String hint) {
    return "insert into t1" + hint + " values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4')";
  }
}
