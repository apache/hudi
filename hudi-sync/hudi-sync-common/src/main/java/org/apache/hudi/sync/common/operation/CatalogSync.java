package org.apache.hudi.sync.common.operation;

import org.apache.hudi.common.util.Option;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public interface CatalogSync {
  /**
   * Create the table.
   *
   * @param tableName         The table name.
   * @param storageSchema     The table schema.
   * @param inputFormatClass  The input format class of this table.
   * @param outputFormatClass The output format class of this table.
   * @param serdeClass        The serde class of this table.
   * @param serdeProperties   The serde properties of this table.
   * @param tableProperties   The table properties for this table.
   */
  void createTable(String tableName, MessageType storageSchema,
                   String inputFormatClass, String outputFormatClass,
                   String serdeClass, Map<String, String> serdeProperties,
                   Map<String, String> tableProperties);

  /**
   * @deprecated Use {@link #tableExists} instead.
   */
  @Deprecated
  boolean doesTableExist(String tableName);

  boolean tableExists(String tableName);

  Option<String> getLastCommitTimeSynced(String tableName);

  void updateLastCommitTimeSynced(String tableName);

  void updateTableProperties(String tableName, Map<String, String> tableProperties);

}
