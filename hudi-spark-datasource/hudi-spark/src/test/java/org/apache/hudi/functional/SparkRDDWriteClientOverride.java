package org.apache.hudi.functional;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;

// Sole purpose of this class is to provide access to otherwise API inaccessible from the tests.
// While it's certainly not a great pattern, it would require substantial test restructuring to
// eliminate such access to an internal API, so this is considered acceptable given it's very limited
// scope (w/in the current package)
class SparkRDDWriteClientOverride extends org.apache.hudi.client.SparkRDDWriteClient {

  public SparkRDDWriteClientOverride(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    super(context, clientConfig);
  }

  @Override
  public void rollbackFailedBootstrap() {
    super.rollbackFailedBootstrap();
  }
}

