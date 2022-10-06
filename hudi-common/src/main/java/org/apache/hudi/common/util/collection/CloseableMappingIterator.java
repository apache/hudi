package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.ClosableIterator;

import java.util.function.Function;

// TODO java-doc
public class CloseableMappingIterator<I, O> extends MappingIterator<I, O> implements ClosableIterator<O> {

  public CloseableMappingIterator(ClosableIterator<I> source, Function<I, O> mapper) {
    super(source, mapper);
  }

  @Override
  public void close() {
    ((ClosableIterator<I>) source).close();
  }
}
