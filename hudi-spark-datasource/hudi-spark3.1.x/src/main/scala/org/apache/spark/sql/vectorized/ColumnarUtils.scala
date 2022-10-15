package org.apache.spark.sql.vectorized

import org.apache.spark.sql.catalyst.InternalRow

object ColumnarUtils {

  /**
   * Utility verifying whether provided instance of [[InternalRow]] is actually
   * an instance of [[ColumnarBatchRow]]
   *
   * NOTE: This utility is required, since in Spark <= 3.3 [[ColumnarBatchRow]] is package-private
   */
  def isColumnarBatchRow(r: InternalRow): Boolean = r.isInstanceOf[ColumnarBatchRow]

}
