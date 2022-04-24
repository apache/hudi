package org.apache.hudi.common.util.queue;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.function.Function;

public class DisruptorMessageQueue<I, O> extends HoodieMessageQueue<I, O> {

  private final Disruptor<HoodieDisruptorEvent<O>> queue;
  private final Function<I, O> transformFunction;
  private RingBuffer<HoodieDisruptorEvent<O>> ringBuffer;

  public DisruptorMessageQueue(int bufferSize, Function<I, O> transformFunction, String waitStrategyName, int producerNumber, Runnable preExecuteRunnable) {
    WaitStrategy waitStrategy = WaitStrategyFactory.build(waitStrategyName);
    HoodieDaemonThreadFactory threadFactory = new HoodieDaemonThreadFactory(preExecuteRunnable);

    if (producerNumber > 1) {
      this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, threadFactory, ProducerType.MULTI, waitStrategy);
    } else {
      this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, threadFactory, ProducerType.SINGLE, waitStrategy);
    }

    this.ringBuffer = queue.getRingBuffer();
    this.transformFunction = transformFunction;
  }

  @Override
  public long size() {
    return queue.getBufferSize();
  }

  @Override
  public void insertRecord(I value) throws Exception {
    O applied = transformFunction.apply(value);

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

  public boolean isEmpty() {
    return ringBuffer.getBufferSize() == ringBuffer.remainingCapacity();
  }
}
