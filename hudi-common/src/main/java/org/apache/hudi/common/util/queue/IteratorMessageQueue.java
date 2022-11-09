package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class IteratorMessageQueue<I, O> extends HoodieIterableMessageQueue<I, O> {

  private static final Logger LOG = LogManager.getLogger(IteratorMessageQueue.class);
  private final Iterator<I> inputItr;
  private final InnerIterator innerIterator;
  private final Function<I, O> transformFunction;
  private final AtomicBoolean isWriteDone = new AtomicBoolean(false);
  private final AtomicInteger count = new AtomicInteger(0);

  public IteratorMessageQueue(Iterator<I> inputItr, Function<I, O> transformFunction) {
    this.inputItr = inputItr;
    this.transformFunction = transformFunction;
    this.innerIterator = new InnerIterator();
  }

  @Override
  public Iterator<O> iterator() {
    return innerIterator;
  }

  @Override
  public long size() {
    return count.get();
  }

  @Override
  public boolean isEmpty() {
    return innerIterator.hasNext();
  }

  @Override
  public void close() throws IOException {
    while (!isWriteDone.get()) {
      isWriteDone.compareAndSet(false, true);
    }
  }

  @Override
  public void insertRecord(I t) throws Exception {
    // no action is needed here.
  }

  @Override
  public Option<O> readNextRecord() {
    return null;
  }

  @Override
  public void markAsFailed(Throwable e) {
    // do nothing.
  }

  /**
   * Iterator for the memory bounded queue.
   */
  private final class InnerIterator implements Iterator<O> {

    @Override
    public boolean hasNext() {
      return inputItr.hasNext();
    }

    @Override
    public O next() {
      if (isWriteDone.get()) {
        throw new IllegalStateException("Queue closed for getting new entries");
      }

      count.incrementAndGet();
      return transformFunction.apply(inputItr.next());
    }
  }
}
