package org.apache.hudi.functional

import org.apache.hudi.execution.bulkinsert.PreCombineRow
import org.apache.spark.sql.Row

/**
 * Simple Precombine Row used in tests.
 */
class SimplePreCombineRow extends PreCombineRow {
  /**
   * Pre combines two Rows. Chooses the one based on impl. Will be used in deduping.
   *
   * @param v1 first value to be combined.
   * @param v2 second value to be combined.
   * @return the combined value.
   */
  override def combineTwoRows(v1: Row, v2: Row): Row = {
    val tsV1 : Long = v1.getAs("ts")
    val tsV2 : Long = v2.getAs("ts")
    if (tsV1 >= tsV2) v1 else v2
  }
}
