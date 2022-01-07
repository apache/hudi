package org.apache.hudi.common.model;

/**
 * Hudi table could be queried in one of the 3 following ways:
 *
 * <ol>
 *   <li>Snapshot: snapshot of the table at the given (latest if not provided) instant is queried</li>
 *   <li>Read Optimized (MOR only): snapshot of the table at the given (latest if not provided)
 *   instant is queried, but w/o reading any of the delta-log files (only reading base-files)</li>
 *   <li>Incremental: only records added w/in the given time-window (defined by beginning and ending instant)
 *   are queried</li>
 * </ol>
 */
public enum HoodieTableQueryType {
  QUERY_TYPE_SNAPSHOT,
  QUERY_TYPE_INCREMENTAL,
  QUERY_TYPE_READ_OPTIMIZED
}
