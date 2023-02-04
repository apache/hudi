package org.apache.hudi.utilities.sources.processor.canal;

public enum PreCombineFieldType {
  /**
   * Not a timestamp type field
   */
  NON_TIMESTAMP,

  /**
   * Timestamp type field in string format.
   */
  DATE_STRING,

  /**
   * Timestamp type field in UNIX_TIMESTAMP format.
   */
  UNIX_TIMESTAMP,

  /**
   * Timestamp type field in EPOCHMILLISECONDS format.
   */
  EPOCHMILLISECONDS
}
