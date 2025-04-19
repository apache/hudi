package org.apache.hudi.data;

import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

class CloseableIteratorListener implements TaskCompletionListener {
  private static final Logger LOG = LoggerFactory.getLogger(CloseableIteratorListener.class);
  private final Iterator<?> iterator;

  CloseableIteratorListener(Iterator<?> iterator) {
    this.iterator = iterator;
  }

  /**
   * Closes the iterator if it also implements {@link AutoCloseable}, otherwise it is a no-op.
   *
   * @param context the spark context
   */
  @Override
  public void onTaskCompletion(TaskContext context) {
    if (iterator instanceof AutoCloseable) {
      try {
        ((AutoCloseable) iterator).close();
      } catch (Exception ex) {
        LOG.warn("Failed to properly close iterator", ex);
      }
    }
  }
}
