package org.apache.hudi.helper;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.connect.writers.ConnectTransactionServices;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Helper class for {@link ConnectTransactionServices} to generate
 * a unique commit time for testing purposes.
 */
public class MockConnectTransactionServices implements ConnectTransactionServices {

  private int commitTime;

  public MockConnectTransactionServices() {
    commitTime = 100;
  }

  @Override
  public String startCommit() {
    commitTime++;
    return String.valueOf(commitTime);
  }

  @Override
  public void endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {
    assertEquals(String.valueOf(this.commitTime), commitTime);
  }

  @Override
  public Map<String, String> loadLatestCommitMetadata() {
    return new HashMap<>();
  }
}
