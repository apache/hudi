package org.apache.hudi.sync.datahub;

import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Handle responses to requests to Datahub Metastore. Just logs them.
 */
public class DatahubResponseLogger implements Callback {
  private static final Logger LOG = LogManager.getLogger(DatahubResponseLogger.class);

  @Override
  public void onCompletion(MetadataWriteResponse response) {
    LOG.info("Completed Datahub RestEmitter request. " +
            "Status: " + (response.isSuccess() ? " succeeded" : " failed"));
    if (!response.isSuccess()) {
      LOG.error("Request failed. " + response);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Response details: " + response);
    }
  }

  @Override
  public void onFailure(Throwable e) {
    LOG.error("Error during Datahub RestEmitter request", e);
  }

}
