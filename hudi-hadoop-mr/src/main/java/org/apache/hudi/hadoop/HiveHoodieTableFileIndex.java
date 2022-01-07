package org.apache.hudi.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.AbstractHoodieTableFileIndex;
import org.apache.hudi.FileStatusCacheTrait;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.collection.JavaConverters;

import java.util.List;

/**
 * TODO java-doc
 */
public class HiveHoodieTableFileIndex extends AbstractHoodieTableFileIndex {

  public static final Logger LOG = LoggerFactory.getLogger(HiveHoodieTableFileIndex.class);

  public HiveHoodieTableFileIndex(HoodieEngineContext engineContext,
                                  HoodieTableMetaClient metaClient,
                                  TypedProperties configProperties,
                                  List<Path> queryPaths,
                                  Option<String> specifiedQueryInstant) {
    super(engineContext,
        metaClient,
        configProperties,
        JavaConverters.asScalaBuffer(queryPaths),
        toScalaOption(specifiedQueryInstant),
        new NoopCache());
  }

  private static scala.Option<String> toScalaOption(Option<String> opt) {
    return scala.Option.apply(opt.orElse(null));
  }

  @Override
  public Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath) {
    // NOTE: Parsing partition path into partition column values isn't required on Hive,
    //       since Hive does partition pruning in a different way (based on the input-path being
    //       fetched by the query engine)
    return new Object[0];
  }

  @Override
  public void logInfo(Function0<String> lazyStr) {
    LOG.info(lazyStr.apply());
  }

  @Override
  public void logWarning(Function0<String> lazyStr) {
    LOG.info(lazyStr.apply());
  }

  static class NoopCache implements FileStatusCacheTrait {
    @Override
    public scala.Option<FileStatus[]> get(Path path) {
      return scala.Option.empty();
    }

    @Override
    public void put(Path path, FileStatus[] leafFiles) {
      // no-op
    }

    @Override
    public void invalidate() {
      // no-op
    }
  }
}
