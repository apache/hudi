package org.apache.hudi.connect.writers;

import org.apache.hudi.client.WriteStatus;

import java.util.List;
import java.util.Map;

public interface TransactionServices {

  String startCommit();

  void endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata);

  Map<String, String> loadLatestCommitMetadata();
}
