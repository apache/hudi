package org.apache.hudi.utilities.sources;

import java.util.Arrays;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JDBCSource extends RowSource {

  private Properties jdbcConnectionProperties = new Properties();
  private static FSDataInputStream passwordFileStream = null;


  public JDBCSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    try {

      DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.URL, Config.DRIVER_CLASS, Config.USER,
          Config.PASSWORD_FILE, Config.RDBMS_TABLE_NAME, Config.IS_INCREMENTAL));

      Option<String> beginInstant =
          lastCkptStr.isPresent() ? lastCkptStr.get().isEmpty() ? Option.empty() : lastCkptStr : Option.empty();

      //To discuss with VC about Checkpoints

      Dataset<Row> jdbcDf = sparkSession.read()
          .jdbc(props.getString(Config.URL), Config.RDBMS_TABLE_NAME, prepareJDBCConnectionProperties(props));
      //To discuss with VC about lowerbound, upperbound and parititon
      // To discuss with VC about handling incremental loading

      return Pair.of(Option.of(jdbcDf), beginInstant.orElseGet(() -> ""));
    } catch (Exception e) {
      return Pair.of(Option.empty(),null);
    }
    
  }

  protected static class Config {
    /**
     * {@value #URL} is the jdbc url for the Hoodie datasource
     */
    private static final String URL = "hoodie.datasource.jdbc.url";

    /**
     * {@value #USER} is the username used for JDBC connection
     */
    private static final String USER = "hoodie.datasource.jdbc.user";

    /**
     * {@value #PASSWORD_FILE} is the base-path for the JDBC password file
     */
    private static final String PASSWORD_FILE = "hoodie.datasource.jdbc.password.file";

    /**
     * {@value #PASSWORD} is the password used to jdbc connection
     */
    private static final String PASSWORD = "hoodie.datasource.jdbc.password";

    /**
     * {@value #DRIVER_CLASS} used for JDBC connection
     */
    private static final String DRIVER_CLASS = "hoodie.datasource.jdbc.driver.class";

    /**
     * {@value #RDBMS_TABLE_NAME} RDBMS table to pull
     */
    private static final String RDBMS_TABLE_NAME = "hoodie.datasource.jdbc.table.name";

    /**
     * {@value #INCREMENTAL_COLUMN} if ran in incremental mode, this field will be used to pull
     * new data incrementally
     */
    private static final String INCREMENTAL_COLUMN = "hoodie.datasource.jdbc.table.incremental.column.name";

    /**
     * {@value #INTERVAL} regular interval for which DeltaStreamer will be scheduled
     */
    private static final String INTERVAL = "hoodie.datasource.jdbc.table.incremental.pull.interval";


    /**
     * {@value #IS_INCREMENTAL} will the JDBC source do an incremental pull?
     */
    private static final String IS_INCREMENTAL = "hoodie.datasource.jdbc.incremental.pull";


  }

  private static Properties prepareJDBCConnectionProperties(TypedProperties properties) throws HoodieException {
    try {
      properties.put(Config.USER,properties.getString(Config.USER));
      FileSystem fileSystem = FileSystem.get(new Configuration());
      passwordFileStream = fileSystem.open(new Path(properties.getString(Config.PASSWORD_FILE)));
      byte[] bytes = new byte[passwordFileStream.available()];
      passwordFileStream.read(bytes);
      properties.put(Config.PASSWORD,new String(bytes));
      return properties;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      IOUtils.closeStream(passwordFileStream);
    }

  }
}
