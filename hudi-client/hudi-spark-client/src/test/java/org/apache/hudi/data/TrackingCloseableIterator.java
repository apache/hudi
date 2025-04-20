package org.apache.hudi.data;

import org.apache.hudi.common.util.collection.ClosableIterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class TrackingCloseableIterator<T> implements ClosableIterator<T>, Serializable {
  private static final Map<String, Boolean> IS_CLOSED_BY_ID = new HashMap<>();
  private final String id;
  private final Iterator<T> inner;

  public TrackingCloseableIterator(String id, Iterator<T> inner) {
    this.id = id;
    this.inner = inner;
    IS_CLOSED_BY_ID.put(id, false);
  }

  public static boolean isClosed(String id) {
    return IS_CLOSED_BY_ID.get(id);
  }

  @Override
  public void close() {
    IS_CLOSED_BY_ID.put(id, true);
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
