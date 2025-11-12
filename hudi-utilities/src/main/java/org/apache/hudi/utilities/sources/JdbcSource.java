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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.SqlQueryBuilder;
import org.apache.hudi.utilities.config.JdbcSourceConfig;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.stripPrefix;

/**
 * Reads data from RDBMS data sources.
 */

public class JdbcSource extends RowSource {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);
  private static final List<String> DB_LIMIT_CLAUSE = Arrays.asList("mysql", "postgresql", "h2");
  private static final String URI_JDBC_PREFIX = "jdbc:";

  public JdbcSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                    SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  /**
   * Validates all user properties and prepares the {@link DataFrameReader} to read from RDBMS.
   *
   * @param session    The {@link SparkSession}.
   * @param properties The JDBC connection properties and data source options.
   * @return The {@link DataFrameReader} to read from RDBMS
   * @throws HoodieException
   */
  private static DataFrameReader validatePropsAndGetDataFrameReader(final SparkSession session,
                                                                    final TypedProperties properties)
      throws HoodieException {
    DataFrameReader dataFrameReader;
    InputStream passwordFileStream = null;
    try {
      dataFrameReader = session.read().format("jdbc");
      dataFrameReader = dataFrameReader.option(
          Config.URL_PROP, getStringWithAltKeys(properties, JdbcSourceConfig.URL));
      dataFrameReader = dataFrameReader.option(
          Config.USER_PROP, getStringWithAltKeys(properties, JdbcSourceConfig.USER));
      dataFrameReader = dataFrameReader.option(
          Config.DRIVER_PROP, getStringWithAltKeys(properties, JdbcSourceConfig.DRIVER_CLASS));
      dataFrameReader = dataFrameReader.option(
          Config.RDBMS_TABLE_PROP, getStringWithAltKeys(properties, JdbcSourceConfig.RDBMS_TABLE_NAME));

      if (containsConfigProperty(properties, JdbcSourceConfig.PASSWORD)) {
        LOG.info("Reading JDBC password from properties file....");
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP,
            getStringWithAltKeys(properties, JdbcSourceConfig.PASSWORD));
      } else if (containsConfigProperty(properties, JdbcSourceConfig.PASSWORD_FILE)
          && !StringUtils.isNullOrEmpty(getStringWithAltKeys(properties, JdbcSourceConfig.PASSWORD_FILE))) {
        LOG.info("Reading JDBC password from password file {}", getStringWithAltKeys(properties, JdbcSourceConfig.PASSWORD_FILE));
        FileSystem fileSystem = FileSystem.get(session.sparkContext().hadoopConfiguration());
        passwordFileStream = fileSystem.open(new Path(getStringWithAltKeys(properties, JdbcSourceConfig.PASSWORD_FILE)));
        byte[] bytes = new byte[passwordFileStream.available()];
        passwordFileStream.read(bytes);
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, new String(bytes));
      } else {
        throw new IllegalArgumentException(String.format("JDBCSource needs either a %s or %s to connect to RDBMS "
            + "datasource", JdbcSourceConfig.PASSWORD_FILE.key(), JdbcSourceConfig.PASSWORD.key()));
      }

      addExtraJdbcOptions(properties, dataFrameReader);

      if (getBooleanWithAltKeys(properties, JdbcSourceConfig.IS_INCREMENTAL)) {
        checkRequiredConfigProperties(properties, Collections.singletonList(JdbcSourceConfig.INCREMENTAL_COLUMN));
      }
      return dataFrameReader;
    } catch (Exception e) {
      throw new HoodieException("Failed to validate properties", e);
    } finally {
      IOUtils.closeStream(passwordFileStream);
    }
  }

  /**
   * Accepts spark JDBC options from the user in terms of EXTRA_OPTIONS adds them to {@link DataFrameReader} Example: In
   * a normal spark code you would do something like: session.read.format('jdbc') .option(fetchSize,1000)
   * .option(timestampFormat,"yyyy-mm-dd hh:mm:ss")
   * <p>
   * The way to pass these properties to HUDI is through the config file. Any property starting with
   * hoodie.streamer.jdbc.extra.options. will be added.
   * <p>
   * Example: hoodie.streamer.jdbc.extra.options.fetchSize=100
   * hoodie.streamer.jdbc.extra.options.upperBound=1
   * hoodie.streamer.jdbc.extra.options.lowerBound=100
   *
   * @param properties      The JDBC connection properties and data source options.
   * @param dataFrameReader The {@link DataFrameReader} to which data source options will be added.
   */
  private static void addExtraJdbcOptions(TypedProperties properties, DataFrameReader dataFrameReader) {
    Set<Object> objects = properties.keySet();
    for (Object property : objects) {
      String prop = property.toString();
      Option<String> keyOption = stripPrefix(prop, JdbcSourceConfig.EXTRA_OPTIONS);
      if (keyOption.isPresent()) {
        String key = keyOption.get();
        String value = properties.getString(prop);
        if (!StringUtils.isNullOrEmpty(value)) {
          LOG.info("Adding {} -> {} to jdbc options", key, value);
          dataFrameReader.option(key, value);
        }
      }
    }
  }

  @Override
  protected Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) throws HoodieException {
    try {
      checkRequiredConfigProperties(props, Arrays.asList(JdbcSourceConfig.URL, JdbcSourceConfig.DRIVER_CLASS,
          JdbcSourceConfig.USER, JdbcSourceConfig.RDBMS_TABLE_NAME, JdbcSourceConfig.IS_INCREMENTAL));
      return fetch(lastCheckpoint, sourceLimit);
    } catch (HoodieException e) {
      LOG.error("Exception while running JDBCSource ", e);
      throw e;
    } catch (Exception e) {
      LOG.error("Exception while running JDBCSource ", e);
      throw new HoodieException("Error fetching next batch from JDBC source. Last checkpoint: " + lastCheckpoint.orElse(null), e);
    }
  }

  /**
   * Decide to do a full RDBMS table scan or an incremental scan based on the lastCheckpoint. If previous checkpoint
   * value exists then we do an incremental scan with a PPD query or else we do a full scan. In certain cases where the
   * incremental query fails, we fallback to a full scan.
   *
   * @param lastCheckpoint Last checkpoint.
   * @return The pair of {@link Dataset} and current checkpoint.
   */
  private Pair<Option<Dataset<Row>>, Checkpoint> fetch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    Dataset<Row> dataset;
    if (lastCheckpoint.isPresent() && !StringUtils.isNullOrEmpty(lastCheckpoint.get().getCheckpointKey())) {
      dataset = incrementalFetch(lastCheckpoint, sourceLimit);
    } else {
      LOG.info("No checkpoint references found. Doing a full rdbms table fetch");
      dataset = fullFetch(sourceLimit);
    }
    dataset.persist(StorageLevel.fromString(
        getStringWithAltKeys(props, JdbcSourceConfig.STORAGE_LEVEL, "MEMORY_AND_DISK_SER")));
    boolean isIncremental = getBooleanWithAltKeys(props, JdbcSourceConfig.IS_INCREMENTAL);
    Pair<Option<Dataset<Row>>, Checkpoint> pair = Pair.of(Option.of(dataset), checkpoint(dataset, isIncremental, lastCheckpoint));
    dataset.unpersist();
    return pair;
  }

  /**
   * Does an incremental scan with PPQ query prepared on the bases of previous checkpoint.
   *
   * @param lastCheckpoint Last checkpoint.
   *                       Note that the records fetched will be exclusive of the last checkpoint (i.e. incremental column value > lastCheckpoint).
   * @return The {@link Dataset} after incremental fetch from RDBMS.
   */
  private Dataset<Row> incrementalFetch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    try {
      final String ppdQuery = "(%s) rdbms_table";
      final SqlQueryBuilder queryBuilder = SqlQueryBuilder.select("*")
          .from(getStringWithAltKeys(props, JdbcSourceConfig.RDBMS_TABLE_NAME))
          .where(String.format(" %s > '%s'", getStringWithAltKeys(props, JdbcSourceConfig.INCREMENTAL_COLUMN),
              lastCheckpoint.get().getCheckpointKey()));

      if (sourceLimit > 0) {
        URI jdbcURI = URI.create(getStringWithAltKeys(props, JdbcSourceConfig.URL).substring(URI_JDBC_PREFIX.length()));
        if (DB_LIMIT_CLAUSE.contains(jdbcURI.getScheme())) {
          queryBuilder.orderBy(getStringWithAltKeys(props, JdbcSourceConfig.INCREMENTAL_COLUMN)).limit(sourceLimit);
        }
      }
      String query = String.format(ppdQuery, queryBuilder.toString());
      LOG.info("PPD QUERY: {}", query);
      LOG.info("Referenced last checkpoint and prepared new predicate pushdown query for jdbc pull {}", query);
      return validatePropsAndGetDataFrameReader(sparkSession, props).option(Config.RDBMS_TABLE_PROP, query).load();
    } catch (Exception e) {
      LOG.error("Error while performing an incremental fetch. Not all database support the PPD query we generate to do an incremental scan", e);
      if (containsConfigProperty(props, JdbcSourceConfig.FALLBACK_TO_FULL_FETCH)
          && getBooleanWithAltKeys(props, JdbcSourceConfig.FALLBACK_TO_FULL_FETCH)) {
        LOG.warn("Falling back to full scan.");
        return fullFetch(sourceLimit);
      }
      throw e;
    }
  }

  /**
   * Does a full scan on the RDBMS data source.
   *
   * @return The {@link Dataset} after running full scan.
   */
  private Dataset<Row> fullFetch(long sourceLimit) {
    final String ppdQuery = "(%s) rdbms_table";
    final SqlQueryBuilder queryBuilder = SqlQueryBuilder.select("*")
        .from(getStringWithAltKeys(props, JdbcSourceConfig.RDBMS_TABLE_NAME));
    if (sourceLimit > 0) {
      URI jdbcURI = URI.create(getStringWithAltKeys(props, JdbcSourceConfig.URL).substring(URI_JDBC_PREFIX.length()));
      if (DB_LIMIT_CLAUSE.contains(jdbcURI.getScheme())) {
        if (containsConfigProperty(props, JdbcSourceConfig.INCREMENTAL_COLUMN)) {
          queryBuilder.orderBy(getStringWithAltKeys(props, JdbcSourceConfig.INCREMENTAL_COLUMN)).limit(sourceLimit);
        } else {
          queryBuilder.limit(sourceLimit);
        }
      }
    }
    String query = String.format(ppdQuery, queryBuilder.toString());
    return validatePropsAndGetDataFrameReader(sparkSession, props).option(Config.RDBMS_TABLE_PROP, query).load();
  }

  private Checkpoint checkpoint(Dataset<Row> rowDataset, boolean isIncremental, Option<Checkpoint> lastCheckpoint) {
    try {
      if (isIncremental) {
        Column incrementalColumn = rowDataset.col(getStringWithAltKeys(props, JdbcSourceConfig.INCREMENTAL_COLUMN));
        final String max = rowDataset.agg(functions.max(incrementalColumn).cast(DataTypes.StringType)).first().getString(0);
        LOG.info("Checkpointing column {} with value: {}", incrementalColumn, max);
        if (max != null) {
          return new StreamerCheckpointV2(max);
        }
        return lastCheckpoint.isPresent() && !StringUtils.isNullOrEmpty(lastCheckpoint.get().getCheckpointKey())
            ? lastCheckpoint.get() : new StreamerCheckpointV2(StringUtils.EMPTY_STRING);
      } else {
        return new StreamerCheckpointV2(StringUtils.EMPTY_STRING);
      }
    } catch (Exception e) {
      LOG.error("Failed to checkpoint");
      throw new HoodieReadFromSourceException("Failed to checkpoint. Last checkpoint: " + lastCheckpoint.orElse(null), e);
    }
  }

  /**
   * Inner class with config keys.
   */
  protected static class Config {
    private static final String URL_PROP = "url";
    /**
     * {@value #USER_PROP} used internally to build jdbc params.
     */
    private static final String USER_PROP = "user";
    /**
     * {@value #PASSWORD_PROP} used internally to build jdbc params.
     */
    private static final String PASSWORD_PROP = "password";
    /**
     * {@value #DRIVER_PROP} used internally to build jdbc params.
     */
    private static final String DRIVER_PROP = "driver";
    /**
     * {@value #RDBMS_TABLE_PROP} used internally for jdbc.
     */
    private static final String RDBMS_TABLE_PROP = "dbtable";
  }
}
