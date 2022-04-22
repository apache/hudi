package org.apache.hudi.common.util.queue;

import com.lmax.disruptor.BlockingWaitStrategy;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.apache.hudi.common.util.SizeEstimator;

import java.util.function.Function;

public class DisruptorMessageQueue<I, O> extends HoodieMessageQueue<I, O> {

  private final Disruptor<HoodieDisruptorEvent<O>> queue;
  private final Function<I, O> transformFunction;

  private final int bufferSize = 1024;

  public DisruptorMessageQueue(long bufferLimitInBytes, Function<I, O> transformFunction, SizeEstimator<O> sizeEstimator, int producerNumber) {
    if (producerNumber > 1) {
      this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, DaemonThreadFactory.INSTANCE, ProducerType.MULTI, new BlockingWaitStrategy());
    } else {
      this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, DaemonThreadFactory.INSTANCE, ProducerType.SINGLE, new BlockingWaitStrategy());
    }
    this.transformFunction = transformFunction;
  }

  @Override
  public long size() {
    return queue.getBufferSize();
  }

  @Override
  public void insertRecord(I value) throws Exception {
    O applied;
    if (value == null) {
      applied = null;
    } else {
      applied = transformFunction.apply(value);
    }

    EventTranslator<HoodieDisruptorEvent<O>> translator = new EventTranslator<HoodieDisruptorEvent<O>>() {
      @Override
      public void translateTo(HoodieDisruptorEvent<O> event, long sequence) {
        event.set(applied);
      }
    };

    queue.getRingBuffer().publishEvent(translator);
  }

  @Override
  public void close() {
    queue.shutdown();
  }

  public Disruptor<HoodieDisruptorEvent<O>> getInnerQueue() {
    return this.queue;
  }
}
