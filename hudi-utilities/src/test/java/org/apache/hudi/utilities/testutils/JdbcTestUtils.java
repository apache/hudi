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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

/**
 * Helper class used in testing {@link org.apache.hudi.utilities.sources.JdbcSource}.
 */
public class JdbcTestUtils {

  private static final Logger LOG = LogManager.getLogger(JdbcTestUtils.class);

  public static List<HoodieRecord> clearAndInsert(String commitTime, int numRecords, Connection connection, HoodieTestDataGenerator dataGenerator, TypedProperties props)
      throws SQLException {
    execute(connection, "DROP TABLE triprec", "Table does not exists");
    execute(connection, "CREATE TABLE triprec ("
        + "id INT NOT NULL AUTO_INCREMENT(1, 1),"
        + "commit_time VARCHAR(50),"
        + "_row_key VARCHAR(50),"
        + "rider VARCHAR(50),"
        + "driver VARCHAR(50),"
        + "begin_lat DOUBLE PRECISION,"
        + "begin_lon DOUBLE PRECISION,"
        + "end_lat DOUBLE PRECISION,"
        + "end_lon DOUBLE PRECISION,"
        + "fare DOUBLE PRECISION,"
        + "last_insert TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)", "Table already exists");

    return insert(commitTime, numRecords, connection, dataGenerator, props);
  }

  public static List<HoodieRecord> insert(String commitTime, int numRecords, Connection connection, HoodieTestDataGenerator dataGenerator, TypedProperties props) throws SQLException {
    PreparedStatement insertStatement =
        connection.prepareStatement("INSERT INTO triprec ("
            + "commit_time,"
            + "_row_key,"
            + "rider,"
            + "driver,"
            + "begin_lat,"
            + "begin_lon,"
            + "end_lat,"
            + "end_lon,"
            + "fare) "
            + "values(?,?,?,?,?,?,?,?,?)");
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInserts(commitTime, numRecords);

    hoodieRecords
        .stream()
        .map(r -> {
          try {
            return ((GenericRecord) ((HoodieAvroRecord) r).getData()
                .getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA, props).get());
          } catch (IOException e) {
            return null;
          }
        })
        .filter(Objects::nonNull)
        .forEach(record -> {
          try {
            insertStatement.setString(1, commitTime);
            insertStatement.setString(2, record.get("_row_key").toString());
            insertStatement.setString(3, record.get("rider").toString());
            insertStatement.setString(4, record.get("driver").toString());
            insertStatement.setDouble(5, Double.parseDouble(record.get("begin_lat").toString()));
            insertStatement.setDouble(6, Double.parseDouble(record.get("begin_lon").toString()));
            insertStatement.setDouble(7, Double.parseDouble(record.get("end_lat").toString()));
            insertStatement.setDouble(8, Double.parseDouble(record.get("end_lon").toString()));
            insertStatement.setDouble(9, Double.parseDouble(((GenericRecord) record.get("fare")).get("amount").toString()));
            insertStatement.addBatch();
          } catch (SQLException e) {
            LOG.warn(e.getMessage());
          }
        });
    insertStatement.executeBatch();
    close(insertStatement);
    return hoodieRecords;
  }

  public static List<HoodieRecord> update(String commitTime, List<HoodieRecord> inserts, Connection connection, HoodieTestDataGenerator dataGenerator, TypedProperties props)
      throws SQLException, IOException {
    PreparedStatement updateStatement =
        connection.prepareStatement("UPDATE triprec set commit_time=?,"
            + "_row_key=?,"
            + "rider=?,"
            + "driver=?,"
            + "begin_lat=?,"
            + "begin_lon=?,"
            + "end_lat=?,"
            + "end_lon=?,"
            + "fare=?"
            + "where _row_key=?");

    List<HoodieRecord> updateRecords = dataGenerator.generateUpdates(commitTime, inserts);
    updateRecords.stream().map(m -> {
      try {
        return ((HoodieAvroRecord) m).getData().getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA, props).get();
      } catch (IOException e) {
        return null;
      }
    }).filter(Objects::nonNull)
        .map(r -> ((GenericRecord) r))
        .sequential()
        .forEach(r -> {
          try {
            updateStatement.setString(1, commitTime);
            updateStatement.setString(2, r.get("_row_key").toString());
            updateStatement.setString(3, r.get("rider").toString());
            updateStatement.setString(4, r.get("driver").toString());
            updateStatement.setDouble(5, Double.parseDouble(r.get("begin_lat").toString()));
            updateStatement.setDouble(6, Double.parseDouble(r.get("begin_lon").toString()));
            updateStatement.setDouble(7, Double.parseDouble(r.get("end_lat").toString()));
            updateStatement.setDouble(8, Double.parseDouble(r.get("end_lon").toString()));
            updateStatement.setDouble(9, Double.parseDouble(((GenericRecord) r.get("fare")).get("amount").toString()));
            updateStatement.setString(10, r.get("_row_key").toString());
            updateStatement.addBatch();
          } catch (SQLException e) {
            LOG.warn(e.getMessage());
          }
        });
    updateStatement.executeBatch();
    close(updateStatement);
    return updateRecords;
  }

  private static void execute(Connection connection, String query, String message) {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(query);
    } catch (SQLException e) {
      LOG.error(message);
    }
  }

  private static void close(Statement statement) {
    try {
      if (statement != null) {
        statement.close();
      }
    } catch (SQLException e) {
      LOG.error("Error while closing statement. " + e.getMessage());
    }
  }

  public static void close(Connection connection) {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOG.error("Error while closing connection. " + e.getMessage());
    }
  }

  public static int count(Connection connection, String tableName) {
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery(String.format("select count(*) from %s", tableName));
      rs.next();
      return rs.getInt(1);
    } catch (SQLException e) {
      LOG.warn("Error while counting records. " + e.getMessage());
      return 0;
    }
  }
}
