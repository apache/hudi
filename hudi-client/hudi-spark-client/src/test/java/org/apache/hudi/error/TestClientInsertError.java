package org.apache.hudi.error;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author liujinhui
 * @date 2021/3/3 10:51
 * @comment
 */
public class TestClientInsertError extends HoodieClientTestBase {


  @Test
  public void testInsertError () {

    SparkRDDWriteClient writeClient = getHoodieWriteClient(getConfig());
    String commitTime = writeClient.startCommit();
    List<HoodieRecord> hoodieRecords = dataGen.generateInserts(commitTime, 100);

    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(hoodieRecords, 1);
    JavaRDD<WriteStatus> writeStatusRDD = writeClient.insertError(recordsRDD, commitTime);

    writeStatusRDD.collect();



  }

}
