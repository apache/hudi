package org.apache.hudi.common.data;

import org.apache.hudi.common.util.Either;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Data representation of either a stream or a list of objects, partitioned into one or more chunks.
 *
 * @param <T> object value type.
 */
public abstract class HoodieBaseListData<T> {

  // Either a list of Stream<T> (lazy) or a list of List<T> (eager), one entry per partition.
  protected final Either<List<Stream<T>>, List<List<T>>> partitions;
  protected final boolean lazy;

  // ——— Single-partition constructors ———

  protected HoodieBaseListData(List<T> data, boolean lazy) {
    this.lazy = lazy;
    if (lazy) {
      // one partition backed by a parallel stream
      this.partitions = Either.left(
          Collections.singletonList(data.stream().parallel())
      );
    } else {
      // one partition backed by the list itself
      this.partitions = Either.right(
          Collections.singletonList(data)
      );
    }
  }

  protected HoodieBaseListData(Stream<T> dataStream, boolean lazy) {
    this.lazy = lazy;
    if (lazy) {
      // one partition, keep the stream as-is
      this.partitions = Either.left(
          Collections.singletonList(dataStream)
      );
    } else {
      // eagerly collect into a list, close the stream, then store
      List<T> collected = dataStream.collect(Collectors.toList());
      dataStream.close();
      this.partitions = Either.right(
          Collections.singletonList(collected)
      );
    }
  }

  // ——— Multi-partition constructors (differ by dummy parameter to avoid erasure clash) ———

  /** Partitioned by sub-lists; private to avoid erasure clash with the single-list ctor. */
  @SuppressWarnings("unused")
  protected HoodieBaseListData(List<List<T>> listPartitions, boolean lazy, int dummy) {
    this.lazy = lazy;
    if (lazy) {
      List<Stream<T>> streams = listPartitions.stream()
          .map(List::stream)
          .collect(Collectors.toList());
      this.partitions = Either.left(streams);
    } else {
      this.partitions = Either.right(listPartitions);
    }
  }

  /** Partitioned by sub-streams; private to avoid erasure clash with the single-stream ctor. */
  @SuppressWarnings("unused")
  protected HoodieBaseListData(List<Stream<T>> streamPartitions, boolean lazy, String dummy) {
    this.lazy = lazy;
    if (lazy) {
      this.partitions = Either.left(streamPartitions);
    } else {
      List<List<T>> lists = streamPartitions.stream()
          .map(stream -> {
            try (Stream<T> s = stream) {
              return s.collect(Collectors.toList());
            }
          })
          .collect(Collectors.toList());
      this.partitions = Either.right(lists);
    }
  }

  // ——— Core APIs ———

  /** Flatten across all partitions in order. */
  protected Stream<T> asStream() {
    if (lazy) {
      return partitions
          .asLeft()
          .stream()
          .flatMap(Function.identity());
    } else {
      return partitions
          .asRight()
          .stream()
          .flatMap(List::stream)
          .parallel();
    }
  }

  /** Number of partitions currently held. */
  protected int partitionCount() {
    return lazy
        ? partitions.asLeft().size()
        : partitions.asRight().size();
  }

  /** Test if *all* partitions are empty. */
  protected boolean isEmpty() {
    return (lazy
        ? partitions.asLeft().stream().allMatch(s -> !s.findAny().isPresent())
        : partitions.asRight().stream().allMatch(List::isEmpty));
  }

  /** Total element count across all partitions. */
  protected long count() {
    return (lazy
        ? partitions.asLeft().stream().mapToLong(s -> s.count()).sum()
        : partitions.asRight().stream().mapToLong(List::size).sum());
  }

  /** Collect everything into a single List<T>. */
  protected List<T> collectAsList() {
    return asStream().collect(Collectors.toList());
  }

  // ——— Utility to close iterators created from Streams ———

  static class IteratorCloser implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(IteratorCloser.class);
    private final Iterator<?> iterator;

    IteratorCloser(Iterator<?> iterator) {
      this.iterator = iterator;
    }

    @Override
    public void run() {
      if (iterator instanceof AutoCloseable) {
        try {
          ((AutoCloseable) iterator).close();
        } catch (Exception ex) {
          LOG.warn("Failed to properly close iterator", ex);
        }
      }
    }
  }
}
