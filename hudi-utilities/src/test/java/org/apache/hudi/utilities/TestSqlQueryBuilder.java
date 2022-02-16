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

package org.apache.hudi.utilities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link SqlQueryBuilder}.
 */
public class TestSqlQueryBuilder {

  @Test
  public void testSelect() {
    String sql = SqlQueryBuilder.select("id", "rider", "time")
        .from("trips")
        .join("users").on("trips.rider = users.id")
        .where("(trips.time > 100 or trips.time < 200)")
        .orderBy("id", "time")
        .limit(10).toString();

    assertEquals("select id, rider, time from trips "
        + "join users on trips.rider = users.id "
        + "where (trips.time > 100 or trips.time < 200) "
        + "order by id, time "
        + "limit 10", sql);
  }

  @Test
  public void testIncorrectQueries() {
    assertThrows(IllegalArgumentException.class, () -> SqlQueryBuilder.select().toString());

    assertThrows(IllegalArgumentException.class, () -> SqlQueryBuilder.select("*").from().toString());

    assertThrows(IllegalArgumentException.class, () -> SqlQueryBuilder.select("id").from("trips").where("").toString());

    assertThrows(IllegalArgumentException.class, () -> SqlQueryBuilder.select("id").from("trips").join("").toString());

    assertThrows(IllegalArgumentException.class, () -> SqlQueryBuilder.select("id").from("trips").join("riders").on("").toString());

    assertThrows(IllegalArgumentException.class, () -> SqlQueryBuilder.select("id").from("trips").join("riders").where("id > 0").orderBy().toString());

    assertThrows(IllegalArgumentException.class, () -> SqlQueryBuilder.select("id").from("trips").join("riders").where("id > 0").orderBy("id").limit(-1).toString());
  }
}
