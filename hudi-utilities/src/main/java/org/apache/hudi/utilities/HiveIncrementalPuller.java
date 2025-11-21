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

package org.apache.hudi.utilities;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.exception.HoodieIncrementalPullException;
import org.apache.hudi.utilities.exception.HoodieIncrementalPullSQLException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * Utility to pull data after a given commit, based on the supplied HiveQL and save the delta as another hive temporary
 * table. This temporary table can be further read using {@link org.apache.hudi.utilities.sources.HiveIncrPullSource} and the changes can
 * be applied to the target table.
 * <p>
 * Current Limitations:
 * <p>
 * - Only the source table can be incrementally pulled (usually the largest table) - The incrementally pulled table
 * can't be referenced more than once.
 */
public class HiveIncrementalPuller {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIncrementalPuller.class);

  public static class Config implements Serializable {

    @Parameter(names = {"--hiveUrl"})
    public String hiveJDBCUrl = "jdbc:hive2://localhost:10014/;transportMode=http;httpPath=hs2";
    @Parameter(names = {"--hiveUser"})
    public String hiveUsername = "hive";
    @Parameter(names = {"--hivePass"})
    public String hivePassword = "";
    @Parameter(names = {"--queue"})
    public String yarnQueueName = "hadoop-queue";
    @Parameter(names = {"--tmp"})
    public String hoodieTmpDir = "/app/hoodie/intermediate";
    @Parameter(names = {"--extractSQLFile"}, required = true)
    public String incrementalSQLFile;
    @Parameter(names = {"--sourceDb"}, required = true)
    public String sourceDb;
    @Parameter(names = {"--sourceTable"}, required = true)
    public String sourceTable;
    @Parameter(names = {"--targetDb"})
    public String targetDb;
    @Parameter(names = {"--targetTable"}, required = true)
    public String targetTable;
    @Parameter(names = {"--tmpdb"})
    public String tmpDb = "tmp";
    @Parameter(names = {"--fromCommitTime"})
    public String fromCommitTime;
    @Parameter(names = {"--maxCommits"})
    public int maxCommits = 3;
    @Parameter(names = {"--fsDefaultFs"})
    public String fsDefaultFs = "file:///";
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  static {
    String driverName = "org.apache.hive.jdbc.HiveDriver";
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find " + driverName + " in classpath. ", e);
    }
  }

  private Connection connection;
  protected final Config config;
  private final ST incrementalPullSQLTemplate;

  public HiveIncrementalPuller(Config config) throws IOException {
    this.config = config;
    validateConfig(config);
    String templateContent =
        FileIOUtils.readAsUTFString(this.getClass().getResourceAsStream("/IncrementalPull.sqltemplate"));
    incrementalPullSQLTemplate = new ST(templateContent);
  }

  private void validateConfig(Config config) {
    if (config.maxCommits == -1) {
      config.maxCommits = Integer.MAX_VALUE;
    }
  }

  public void saveDelta() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS",config.fsDefaultFs);
    FileSystem fs = FileSystem.get(conf);
    Statement stmt = null;
    try {
      if (config.fromCommitTime == null) {
        config.fromCommitTime = inferCommitTime(fs);
        LOG.info("FromCommitTime inferred as " + config.fromCommitTime);
      }

      LOG.info("FromCommitTime - " + config.fromCommitTime);
      String sourceTableLocation = getTableLocation(config.sourceDb, config.sourceTable);
      String lastCommitTime = getLastCommitTimePulled(fs, sourceTableLocation);
      if (lastCommitTime == null) {
        LOG.info("Nothing to pull. However we will continue to create a empty table");
        lastCommitTime = config.fromCommitTime;
      }

      Connection conn = getConnection();
      stmt = conn.createStatement();
      // drop the temp table if exists
      String tempDbTable = config.tmpDb + "." + config.targetTable + "__" + config.sourceTable;
      String tempDbTablePath =
          config.hoodieTmpDir + "/" + config.targetTable + "__" + config.sourceTable + "/" + lastCommitTime;
      executeStatement("drop table if exists " + tempDbTable, stmt);
      deleteHDFSPath(fs, tempDbTablePath);
      if (!ensureTempPathExists(fs, lastCommitTime)) {
        throw new IllegalStateException("Could not create target path at "
            + new Path(config.hoodieTmpDir, config.targetTable + "/" + lastCommitTime));
      }

      initHiveBeelineProperties(stmt);
      executeIncrementalSQL(tempDbTable, tempDbTablePath, stmt);
      LOG.info("Finished HoodieReader execution");
    } catch (SQLException e) {
      LOG.error("Exception when executing SQL", e);
      throw new IOException("Could not scan " + config.sourceTable + " incrementally", e);
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        LOG.error("Could not close the resultSet opened ", e);
      }
    }
  }

  private void executeIncrementalSQL(String tempDbTable, String tempDbTablePath, Statement stmt)
      throws FileNotFoundException, SQLException {
    incrementalPullSQLTemplate.add("tempDbTable", tempDbTable);
    incrementalPullSQLTemplate.add("tempDbTablePath", tempDbTablePath);

    String storedAsClause = getStoredAsClause();

    incrementalPullSQLTemplate.add("storedAsClause", storedAsClause);
    String incrementalSQL = new Scanner(new File(config.incrementalSQLFile)).useDelimiter("\\Z").next();
    if (!incrementalSQL.contains(config.sourceDb + "." + config.sourceTable)) {
      LOG.error("Incremental SQL does not have " + config.sourceDb + "." + config.sourceTable
          + ", which means its pulling from a different table. Fencing this from happening.");
      throw new HoodieIncrementalPullSQLException(
          "Incremental SQL does not have " + config.sourceDb + "." + config.sourceTable);
    }
    if (!incrementalSQL.contains("`_hoodie_commit_time` > '%s'")) {
      LOG.error("Incremental SQL : " + incrementalSQL
          + " does not contain `_hoodie_commit_time` > '%s'. Please add "
          + "this clause for incremental to work properly.");
      throw new HoodieIncrementalPullSQLException(
          "Incremental SQL does not have clause `_hoodie_commit_time` > '%s', which "
              + "means its not pulling incrementally");
    }

    incrementalPullSQLTemplate.add("incrementalSQL", String.format(incrementalSQL, config.fromCommitTime));
    String sql = incrementalPullSQLTemplate.render();
    // Check if the SQL is pulling from the right database
    executeStatement(sql, stmt);
  }

  private String getStoredAsClause() {
    return "STORED AS AVRO";
  }

  private void initHiveBeelineProperties(Statement stmt) throws SQLException {
    LOG.info("Setting up Hive JDBC Session with properties");
    // set the queue
    executeStatement("set mapred.job.queue.name=" + config.yarnQueueName, stmt);
    // Set the inputFormat to HoodieCombineHiveInputFormat
    executeStatement("set hive.input.format=org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat", stmt);
    // Allow queries without partition predicate
    executeStatement("set hive.strict.checks.large.query=false", stmt);
    // Don't gather stats for the table created
    executeStatement("set hive.stats.autogather=false", stmt);
    // Set the hoodie mode
    executeStatement("set hoodie." + config.sourceTable + ".consume.mode=INCREMENTAL", stmt);
    // Set the from commit time
    executeStatement("set hoodie." + config.sourceTable + ".consume.start.timestamp=" + config.fromCommitTime, stmt);
    // Set number of commits to pull
    executeStatement("set hoodie." + config.sourceTable + ".consume.max.commits=" + config.maxCommits, stmt);
  }

  private boolean deleteHDFSPath(FileSystem fs, String path) throws IOException {
    LOG.info("Deleting path " + path);
    return fs.delete(new Path(path), true);
  }

  private void executeStatement(String sql, Statement stmt) throws SQLException {
    LOG.info("Executing: " + sql);
    stmt.execute(sql);
  }

  private String inferCommitTime(FileSystem fs) throws IOException {
    LOG.info("FromCommitTime not specified. Trying to infer it from Hoodie table " + config.targetDb + "."
        + config.targetTable);
    String targetDataLocation = getTableLocation(config.targetDb, config.targetTable);
    return scanForCommitTime(fs, targetDataLocation);
  }

  private String getTableLocation(String db, String table) {
    ResultSet resultSet = null;
    Statement stmt = null;
    try {
      Connection conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery("describe formatted `" + db + "." + table + "`");
      while (resultSet.next()) {
        if (resultSet.getString(1).trim().equals("Location:")) {
          LOG.info("Inferred table location for " + db + "." + table + " as " + resultSet.getString(2));
          return resultSet.getString(2);
        }
      }
    } catch (SQLException e) {
      throw new HoodieIncrementalPullException("Failed to get data location for table " + db + "." + table, e);
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (resultSet != null) {
          resultSet.close();
        }
      } catch (SQLException e) {
        LOG.error("Could not close the resultSet opened ", e);
      }
    }
    return null;
  }

  private String scanForCommitTime(FileSystem fs, String targetDataPath) throws IOException {
    if (targetDataPath == null) {
      throw new IllegalArgumentException("Please specify either --fromCommitTime or --targetDataPath");
    }
    if (!fs.exists(new Path(targetDataPath)) || !fs.exists(new Path(targetDataPath + "/.hoodie"))) {
      return "0";
    }
    HoodieTableMetaClient metadata = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(fs.getConf())).setBasePath(targetDataPath).build();

    Option<HoodieInstant> lastCommit =
        metadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
    if (lastCommit.isPresent()) {
      return lastCommit.get().requestedTime();
    }
    return "0";
  }

  private boolean ensureTempPathExists(FileSystem fs, String lastCommitTime) throws IOException {
    Path targetBaseDirPath = new Path(config.hoodieTmpDir, config.targetTable + "__" + config.sourceTable);
    if (!fs.exists(targetBaseDirPath)) {
      LOG.info("Creating " + targetBaseDirPath + " with permission drwxrwxrwx");
      boolean result =
          FileSystem.mkdirs(fs, targetBaseDirPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      if (!result) {
        throw new HoodieException("Could not create " + targetBaseDirPath + " with the required permissions");
      }
    }

    Path targetPath = new Path(targetBaseDirPath, lastCommitTime);
    if (fs.exists(targetPath)) {
      boolean result = fs.delete(targetPath, true);
      if (!result) {
        throw new HoodieException("Could not delete existing " + targetPath);
      }
    }
    LOG.info("Creating " + targetPath + " with permission drwxrwxrwx");
    return FileSystem.mkdirs(fs, targetBaseDirPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
  }

  private String getLastCommitTimePulled(FileSystem fs, String sourceTableLocation) {
    HoodieTableMetaClient metadata = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(fs.getConf()))
        .setBasePath(sourceTableLocation).build();
    List<String> commitsToSync = metadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .findInstantsAfter(config.fromCommitTime, config.maxCommits).getInstantsAsStream().map(HoodieInstant::requestedTime)
        .collect(Collectors.toList());
    if (commitsToSync.isEmpty()) {
      LOG.info("Nothing to sync. All commits in {} are {} and from commit time is {}", config.sourceTable, metadata.getActiveTimeline().getCommitsTimeline()
          .filterCompletedInstants().getInstants(), config.fromCommitTime);
      return null;
    }
    LOG.info("Syncing commits {}", commitsToSync);
    return commitsToSync.get(commitsToSync.size() - 1);
  }

  private Connection getConnection() throws SQLException {
    if (connection == null) {
      LOG.info("Getting Hive Connection to {}", config.hiveJDBCUrl);
      this.connection = DriverManager.getConnection(config.hiveJDBCUrl, config.hiveUsername, config.hivePassword);

    }
    return connection;
  }

  public static void main(String[] args) throws IOException {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    new HiveIncrementalPuller(cfg).saveDelta();
  }
}
