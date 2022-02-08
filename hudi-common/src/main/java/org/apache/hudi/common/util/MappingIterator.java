package org.apache.hudi.common.util;

import java.util.Iterator;
import java.util.function.Function;

/**
 * TODO
 */
public class MappingIterator<T, R> implements Iterator<R> {

  private final Iterator<T> sourceIterator;
  private final Function<T, R> mapper;

  public MappingIterator(Iterator<T> sourceIterator, Function<T, R> mapper) {
    this.sourceIterator = sourceIterator;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext() {
    return sourceIterator.hasNext();
  }

  @Override
  public R next() {
    return mapper.apply(sourceIterator.next());
  }
}
