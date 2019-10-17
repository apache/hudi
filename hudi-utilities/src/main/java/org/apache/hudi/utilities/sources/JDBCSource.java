package org.apache.hudi.utilities.sources;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSource extends RowSource {

  private static Logger LOG = LoggerFactory.getLogger(JDBCSource.class);


  public JDBCSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  private static DataFrameReader validatePropsAndGetDataFrameReader(final SparkSession session,
      final TypedProperties properties)
      throws HoodieException {
    FSDataInputStream passwordFileStream = null;
    try {
      DataFrameReader dataFrameReader = session.read().format("jdbc");
      dataFrameReader = dataFrameReader.option(Config.URL_PROP, properties.getString(Config.URL));
      dataFrameReader = dataFrameReader.option(Config.USER_PROP, properties.getString(Config.USER));
      dataFrameReader = dataFrameReader.option(Config.DRIVER_PROP, properties.getString(Config.DRIVER_CLASS));
      dataFrameReader = dataFrameReader
          .option(Config.RDBMS_TABLE_PROP, properties.getString(Config.RDBMS_TABLE_NAME));

      if (!properties.containsKey(Config.PASSWORD)) {
        if (properties.containsKey(Config.PASSWORD_FILE)) {
          if (!StringUtils.isNullOrEmpty(properties.getString(Config.PASSWORD_FILE))) {
            LOG.info("Reading password for password file {}", properties.getString(Config.PASSWORD_FILE));
            FileSystem fileSystem = FileSystem.get(new Configuration());
            passwordFileStream = fileSystem.open(new Path(properties.getString(Config.PASSWORD_FILE)));
            byte[] bytes = new byte[passwordFileStream.available()];
            passwordFileStream.read(bytes);
            dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, new String(bytes));
          } else {
            throw new IllegalArgumentException(
                String.format("%s property cannot be null or empty", Config.PASSWORD_FILE));
          }
        } else {
          throw new IllegalArgumentException(String.format("JDBCSource needs either a %s or %s to connect to RDBMS "
              + "datasource", Config.PASSWORD_FILE, Config.PASSWORD));
        }
      } else if (!StringUtils.isNullOrEmpty(properties.getString(Config.PASSWORD))) {
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, properties.getString(Config.PASSWORD));
      } else {
        throw new IllegalArgumentException(String.format("%s cannot be null or empty. ", Config.PASSWORD));
      }
      if (properties.containsKey(Config.EXTRA_OPTIONS)) {
        if (!StringUtils.isNullOrEmpty(properties.getString(Config.EXTRA_OPTIONS))) {
          LOG.info("Setting {}", Config.EXTRA_OPTIONS);
          String[] options = properties.getString(Config.EXTRA_OPTIONS).split(",");
          for (String option : options) {
            if (!StringUtils.isNullOrEmpty(option)) {
              String[] kv = option.split("=");
              if (kv.length == 2) {
                dataFrameReader = dataFrameReader.option(kv[0], kv[1]);
                LOG.info("{} = {} has been set for JDBC pull ", kv[0], kv[1]);
              } else {
                LOG.warn("Option {} not set because it does not have a value", kv[0], new IllegalArgumentException(
                    String.format(" %s should have a corresponding value separated by \"=\"", kv[0])));
              }
            }
          }
        }
      }
      if (properties.getBoolean(Config.IS_INCREMENTAL)) {
        DataSourceUtils.checkRequiredProperties(properties, Arrays.asList(Config.INCREMENTAL_COLUMN));
      }
      return dataFrameReader;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      IOUtils.closeStream(passwordFileStream);
    }
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    final String ppdQuery = "(select * from %s where %s >= \" %s \") rdbms_table";
    try {
      DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.URL, Config.DRIVER_CLASS, Config.USER,
          Config.RDBMS_TABLE_NAME, Config.IS_INCREMENTAL));

      Option<String> lastCheckpoint =
          lastCkptStr.isPresent() ? lastCkptStr.get().isEmpty() ? Option.empty() : lastCkptStr : Option.empty();
      boolean isIncremental = props.getBoolean(Config.IS_INCREMENTAL);
      if (lastCheckpoint.equals(Option.empty())) {
        LOG.info("No previous checkpoints found.. ");
        Dataset<Row> rowDataset = validatePropsAndGetDataFrameReader(sparkSession, props).load();
        return sendDfAndCheckpoint(rowDataset, isIncremental);
      } else {
        if (StringUtils.isNullOrEmpty(lastCheckpoint.get())) {
          LOG.warn("Previous checkpoint entry was null or empty. Falling back to full jdbc pull.");
          Dataset<Row> rowDataset = validatePropsAndGetDataFrameReader(sparkSession, props).load();
          return sendDfAndCheckpoint(rowDataset, isIncremental);
        } else {
          String query = String
              .format(ppdQuery, props.getString(Config.RDBMS_TABLE_NAME), props.getString(Config.INCREMENTAL_COLUMN),
                  lastCheckpoint.get());
          LOG.info("Referenced last checkpoint and prepared new predicate pushdown query for jdbc pull {}", query);
          Dataset<Row> rowDataset = validatePropsAndGetDataFrameReader(sparkSession, props)
              .option(props.getString(Config.RDBMS_TABLE_PROP), query).load();
          return sendDfAndCheckpoint(rowDataset, isIncremental);

        }
      }
    } catch (Exception e) {
      LOG.error("Exception while running JDBCSource ", e);
      return Pair.of(Option.empty(), null);
    }
  }

  @NotNull
  private Pair<Option<Dataset<Row>>, String> sendDfAndCheckpoint(Dataset<Row> rowDataset, boolean isIncremental) {
    if (isIncremental) {
      Column incrementalColumn = rowDataset.col(props.getString(Config.INCREMENTAL_COLUMN));
      final String max = rowDataset.agg(functions.max(incrementalColumn).cast(DataTypes.StringType)).first()
          .getString(0);
      LOG.info("Sending {} with checkpoint val {} ", incrementalColumn, max);
      return Pair.of(Option.of(rowDataset), max);
    } else {
      return Pair.of(Option.of(rowDataset), null);
    }
  }


  protected static class Config {

    /**
     * {@value #URL} is the jdbc url for the Hoodie datasource
     */
    private static final String URL = "hoodie.datasource.jdbc.url";

    private static final String URL_PROP = "url";

    /**
     * {@value #USER} is the username used for JDBC connection
     */
    private static final String USER = "hoodie.datasource.jdbc.user";

    /**
     * {@value #USER_PROP} used internally to build jdbc params
     */
    private static final String USER_PROP = "user";

    /**
     * {@value #PASSWORD} is the password used for JDBC connection
     */
    private static final String PASSWORD = "hoodie.datasource.jdbc.password";

    /**
     * {@value #PASSWORD_FILE} is the base-path for the JDBC password file
     */
    private static final String PASSWORD_FILE = "hoodie.datasource.jdbc.password.file";

    /**
     * {@value #PASSWORD_PROP} used internally to build jdbc params
     */
    private static final String PASSWORD_PROP = "password";

    /**
     * {@value #DRIVER_CLASS} used for JDBC connection
     */
    private static final String DRIVER_CLASS = "hoodie.datasource.jdbc.driver.class";

    /**
     * {@value #DRIVER_PROP} used internally to build jdbc params
     */
    private static final String DRIVER_PROP = "driver";

    /**
     * {@value #RDBMS_TABLE_NAME} RDBMS table to pull
     */
    private static final String RDBMS_TABLE_NAME = "hoodie.datasource.jdbc.table.name";

    /**
     * {@value #RDBMS_TABLE_PROP} used internall for jdbc
     */
    private static final String RDBMS_TABLE_PROP = "dbtable";

    /**
     * {@value #INCREMENTAL_COLUMN} if ran in incremental mode, this field will be used to pull new data incrementally
     */
    private static final String INCREMENTAL_COLUMN = "hoodie.datasource.jdbc.table.incremental.column.name";

    /**
     * {@value #IS_INCREMENTAL} will the JDBC source do an incremental pull?
     */
    private static final String IS_INCREMENTAL = "hoodie.datasource.jdbc.incremental.pull";

    /**
     * {@value #INTERVAL} regular interval for which DeltaStreamer will be scheduled
     */
    private static final String INTERVAL = "hoodie.datasource.jdbc.table.incremental.pull.interval";

    /**
     * {@value #EXTRA_OPTIONS} used to set any extra options the user specifies for jdbc
     */
    private static final String EXTRA_OPTIONS = "hoodie.datasource.jdbc.extra.options";

  }

}
