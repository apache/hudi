/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.cli.utils;

import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.hadoop.HoodieInputFormat;
import org.apache.commons.dbcp.BasicDataSource;
import org.joda.time.DateTime;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveUtil {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    static {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not find " + driverName + " in classpath. ", e);
        }
    }

    private static Connection connection;

    private static Connection getConnection(String jdbcUrl, String user, String pass) throws SQLException {
        DataSource ds = getDatasource(jdbcUrl, user, pass);
        return ds.getConnection();
    }

    private static DataSource getDatasource(String jdbcUrl, String user, String pass) {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(driverName);
        ds.setUrl(jdbcUrl);
        ds.setUsername(user);
        ds.setPassword(pass);
        return ds;
    }

    public static long countRecords(String jdbcUrl, HoodieTableMetadata source, String dbName, String user, String pass) throws SQLException {
        Connection conn = HiveUtil.getConnection(jdbcUrl, user, pass);
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        try {
            //stmt.execute("set mapred.job.queue.name=<queue_name>");
            stmt.execute("set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat" );
            stmt.execute("set hive.stats.autogather=false" );
            System.out.println("Class " + HoodieInputFormat.class.getName());
            rs = stmt.executeQuery(
                "select count(`_hoodie_commit_time`) as cnt from " + dbName + "." + source
                    .getTableName());
            long count = -1;
            if(rs.next()) {
                count = rs.getLong("cnt");
            }
            System.out.println("Total records in " + source.getTableName() + " is " + count);
            return count;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public static long countRecords(String jdbcUrl, HoodieTableMetadata source, String srcDb,
        int partitions, String user, String pass) throws SQLException {
        DateTime dateTime = DateTime.now();
        String endDateStr =
            dateTime.getYear() + "-" + String.format("%02d", dateTime.getMonthOfYear()) + "-" +
                String.format("%02d", dateTime.getDayOfMonth());
        dateTime = dateTime.minusDays(partitions);
        String startDateStr =
            dateTime.getYear() + "-" + String.format("%02d", dateTime.getMonthOfYear()) + "-" +
                String.format("%02d", dateTime.getDayOfMonth());
        System.out.println("Start date " + startDateStr + " and end date " + endDateStr);
        return countRecords(jdbcUrl, source, srcDb, startDateStr, endDateStr, user, pass);
    }

    private static long countRecords(String jdbcUrl, HoodieTableMetadata source, String srcDb, String startDateStr,
        String endDateStr, String user, String pass) throws SQLException {
        Connection conn = HiveUtil.getConnection(jdbcUrl, user, pass);
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        try {
            //stmt.execute("set mapred.job.queue.name=<queue_name>");
            stmt.execute("set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
            stmt.execute("set hive.stats.autogather=false");
            rs = stmt.executeQuery(
                "select count(`_hoodie_commit_time`) as cnt from " + srcDb + "." + source
                    .getTableName() + " where datestr>'" + startDateStr + "' and datestr<='"
                    + endDateStr + "'");
            if(rs.next()) {
                return rs.getLong("cnt");
            }
            return -1;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}
