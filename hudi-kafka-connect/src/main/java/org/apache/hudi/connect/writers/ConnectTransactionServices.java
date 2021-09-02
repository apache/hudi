package org.apache.hudi.connect.writers;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.connect.core.TopicTransactionCoordinator;

import java.util.List;
import java.util.Map;

/**
 * Transaction service APIs used by
 * {@link TopicTransactionCoordinator}.
 */
public interface ConnectTransactionServices {

  String startCommit();

  void endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata);

  Map<String, String> loadLatestCommitMetadata();
}
