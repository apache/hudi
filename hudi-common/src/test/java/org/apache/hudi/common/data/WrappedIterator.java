package org.apache.hudi.common.data;

import org.apache.kafka.common.utils.CloseableIterator;

import java.util.Iterator;

class WrappedIterator<T> implements CloseableIterator<T> {
  private final Iterator<T> inner;
  private boolean isClosed = false;

  public WrappedIterator(Iterator<T> inner) {
    this.inner = inner;
  }

  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void close() {
    isClosed = true;
  }

  @Override
  public boolean hasNext() {
    return inner.hasNext();
  }

  @Override
  public T next() {
    return inner.next();
  }
}
