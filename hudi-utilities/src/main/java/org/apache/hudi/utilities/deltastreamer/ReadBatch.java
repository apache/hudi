package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaRDD;

public class ReadBatch {

  private SchemaProvider schemaProvider;
  private String checkpointStr;
  private JavaRDD<HoodieRecord> hoodieRecordJavaRDD;
  private String schemaStr;

  public ReadBatch(SchemaProvider schemaProvider, String checkpointStr, JavaRDD<HoodieRecord> hoodieRecordJavaRDD, String schemaStr) {
    this.schemaProvider = schemaProvider;
    this.checkpointStr = checkpointStr;
    this.hoodieRecordJavaRDD = hoodieRecordJavaRDD;
    this.schemaStr = schemaStr;
  }

  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  public String getCheckpointStr() {
    return checkpointStr;
  }

  public JavaRDD<HoodieRecord> getHoodieRecordJavaRDD() {
    return hoodieRecordJavaRDD;
  }

  public String getSchemaStr() {
    return schemaStr;
  }
}
