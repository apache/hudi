package org.apache.hudi.client.transaction;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.LockConfiguration;

public class InProcessLockProviderWithRuntimeError extends InProcessLockProvider {

  public InProcessLockProviderWithRuntimeError(
      LockConfiguration lockConfiguration,
      Configuration conf) {
    super(lockConfiguration, conf);
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    throw new RuntimeException();
  }

  @Override
  public void unlock() {
    return;
  }
}
