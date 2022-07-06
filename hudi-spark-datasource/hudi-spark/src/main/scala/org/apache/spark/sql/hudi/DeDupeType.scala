package org.apache.spark.sql.hudi

object DeDupeType extends Enumeration {

  type dedupeType = Value

  val INSERT_TYPE = Value("insert_type")
  val UPDATE_TYPE = Value("update_type")
  val UPSERT_TYPE = Value("upsert_type")
}
