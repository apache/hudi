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

package org.apache.hudi.integ.testsuite.dag.nodes;

import org.apache.hudi.common.util.collection.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public abstract class BaseQueryNode extends DagNode<Boolean> {

  public void setSessionProperties(List<String> properties, Statement stmt) throws SQLException {
    for (String prop : properties) {
      executeStatement(prop, stmt);
    }
  }

  public void executeAndValidateQueries(List<Pair<String, Integer>> queriesWithResult, Statement stmt) throws SQLException {
    for (Pair<String, Integer> queryAndResult : queriesWithResult) {
      log.info("Running {}", queryAndResult.getLeft());
      ResultSet res = stmt.executeQuery(queryAndResult.getLeft());
      if (!res.next()) {
        log.info("res.next() was False - typically this means the query returned no rows.");
        assert 0 == queryAndResult.getRight();
      }
      else {
        Integer result = res.getInt(1);
        if (!queryAndResult.getRight().equals(result)) {
          throw new AssertionError(
              "QUERY: " + queryAndResult.getLeft()
                  + " | EXPECTED RESULT = " + queryAndResult.getRight()
                  + " | ACTUAL RESULT = " + result
          );
        }
      }
      log.info("Successfully validated query!");
    }
  }

  private void executeStatement(String query, Statement stmt) throws SQLException {
    log.info("Executing statement {}", stmt.toString());
    stmt.execute(query);
  }
}
