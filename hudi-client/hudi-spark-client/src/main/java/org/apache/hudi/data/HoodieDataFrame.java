package org.apache.hudi.data;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.storage.StorageLevel;

import java.util.Iterator;
import java.util.List;

public class HoodieDataFrame extends HoodieData<Row> {

  private final Dataset<Row> dataframe;

  private HoodieDataFrame(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
  }

  public static HoodieDataFrame of(Dataset<Row> dataframe) {
    return new HoodieDataFrame(dataframe);
  }

  @Override
  public Dataset<Row> get() {
    return dataframe;
  }

  @Override
  public void persist(String cacheConfig) {
    dataframe.persist(StorageLevel.fromString(cacheConfig));
  }

  @Override
  public void unpersist() {
    dataframe.unpersist();
  }

  @Override
  public boolean isEmpty() {
    return dataframe.isEmpty();
  }

  @Override
  public long count() {
    return dataframe.count();
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<Row, O> func) {
    MapFunction<Row, Row> rowFunc = r -> (Row) func.apply(r);
    HoodieDataFrame hdf = HoodieDataFrame.of(dataframe.map(rowFunc, RowEncoder.apply(dataframe.schema())));
    return (HoodieData<O>) hdf;
  }

  @Override
  public <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<Row>, Iterator<O>> func, boolean preservesPartitioning) {
    MapPartitionsFunction<Row, Row> rowFunc = iter -> (Iterator<Row>) func.apply(iter);
    HoodieDataFrame hdf = HoodieDataFrame.of(dataframe.mapPartitions(rowFunc, RowEncoder.apply(dataframe.schema())));
    return (HoodieData<O>) hdf;
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<Row, Iterator<O>> func) {
    FlatMapFunction<Row, Row> rowFunc = r -> (Iterator<Row>) func.apply(r);
    HoodieDataFrame hdf = HoodieDataFrame.of(dataframe.flatMap(rowFunc, RowEncoder.apply(dataframe.schema())));
    return (HoodieData<O>) hdf;
  }

  @Override
  public <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<Row, K, V> mapToPairFunc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieData<Row> distinct() {
    return HoodieDataFrame.of(dataframe.distinct());
  }

  @Override
  public HoodieData<Row> union(HoodieData<Row> other) {
    return HoodieDataFrame.of(dataframe.union(((HoodieDataFrame)other).get()));
  }

  @Override
  public List<Row> collectAsList() {
    return dataframe.collectAsList();
  }
}
