---
title: Metrics
keywords: [ hudi, administration, operation, devops, metrics]
summary: This section offers an overview of metrics in Hudi
toc: true
last_modified_at: 2020-06-20T15:59:57-04:00
---

In this section, we will introduce the `MetricsReporter` and `HoodieMetrics` in Hudi. You can view the metrics-related configurations [here](configurations#METRICS).

## MetricsReporter

MetricsReporter provides APIs for reporting `HoodieMetrics` to user-specified backends. Currently, the implementations include InMemoryMetricsReporter, JmxMetricsReporter, MetricsGraphiteReporter and DatadogMetricsReporter. Since InMemoryMetricsReporter is only used for testing, we will introduce the other three implementations.

### JmxMetricsReporter

JmxMetricsReporter is an implementation of JMX reporter, which used to report JMX metrics.

#### Configurations
The following is an example of `JmxMetricsReporter`. More detailed configurations can be referenced [here](configurations#Metrics-Configurations-for-Jmx).

  ```properties
  hoodie.metrics.on=true
  hoodie.metrics.reporter.type=JMX
  hoodie.metrics.jmx.host=192.168.0.106
  hoodie.metrics.jmx.port=4001
  ```

#### Demo
As configured above, JmxMetricsReporter will started JMX server on port 4001. We can start a jconsole to connect to 192.168.0.106:4001. Below is an illustration of monitoring Hudi JMX metrics through jconsole.
<figure>
    <img className="docimage" src={require("/assets/images/hudi_jxm_metrics.png").default} alt="hudi_jxm_metrics.png"  />
</figure>

### MetricsGraphiteReporter

MetricsGraphiteReporter is an implementation of Graphite reporter, which connects to a Graphite server, and send `HoodieMetrics` to it.

#### Configurations
The following is an example of `MetricsGraphiteReporter`. More detaile configurations can be referenced [here](configurations#Metrics-Configurations-for-Graphite).

  ```properties
  hoodie.metrics.on=true
  hoodie.metrics.reporter.type=GRAPHITE
  hoodie.metrics.graphite.host=192.168.0.106
  hoodie.metrics.graphite.port=2003
  hoodie.metrics.graphite.metric.prefix=<your metrics prefix>
  ```
#### Demo
As configured above, assuming a Graphite server is running on host 192.168.0.106 and port 2003, a running Hudi job will connect and report metrics data to it. Below is an illustration of monitoring hudi metrics through Graphite.
  <figure>
      <img className="docimage" src={require("/assets/images/hudi_graphite_metrics.png").default} alt="hudi_graphite_metrics.png"  />
  </figure>

### DatadogMetricsReporter

DatadogMetricsReporter is an implementation of Datadog reporter.
A reporter which publishes metric values to Datadog monitoring service via Datadog HTTP API.

#### Configurations
The following is an example of `DatadogMetricsReporter`. More detailed configurations can be referenced [here](configurations#Metrics-Configurations-for-Datadog-reporter).

```properties
hoodie.metrics.on=true
hoodie.metrics.reporter.type=DATADOG
hoodie.metrics.datadog.api.site=EU # or US
hoodie.metrics.datadog.api.key=<your api key>
hoodie.metrics.datadog.metric.prefix=<your metrics prefix>
```

 * `hoodie.metrics.datadog.api.site` will set the Datadog API site, which determines whether the requests will be sent to api.datadoghq.eu (EU) or api.datadoghq.com (US). Set this according to your Datadog account settings.
 * `hoodie.metrics.datadog.api.key` will set the api key.
 * `hoodie.metrics.datadog.metric.prefix` will help segregate metrics by setting different prefixes for different jobs. Note that it will use `.` to delimit the prefix and the metric name. For example, if the prefix is set to `foo`, then `foo.` will be prepended to the metric name.

#### Demo
In this demo, we ran a `HoodieDeltaStreamer` job with `HoodieMetrics` turned on and other configurations set properly.

<figure>
    <img className="docimage" src={require("/assets/images/blog/2020-05-28-datadog-metrics-demo.png").default} alt="hudi_datadog_metrics.png"  />
</figure>

 As shown above, we were able to collect Hudi's action-related metrics like

 * `<prefix>.<table name>.commit.totalScanTime`
 * `<prefix>.<table name>.clean.duration`
 * `<prefix>.<table name>.index.lookup.duration`

 as well as `HoodieDeltaStreamer`-specific metrics

 * `<prefix>.<table name>.deltastreamer.duration`
 * `<prefix>.<table name>.deltastreamer.hiveSyncDuration`

### PrometheusMetricsReporter
[Prometheus](https://prometheus.io/) is an open source systems monitoring and alerting toolkit.
Prometheus has a [PushGateway](https://prometheus.io/docs/practices/pushing/) that Apache Hudi can leverage for metrics reporting.
Follow [Prometheus documentation](https://prometheus.io/docs/introduction/first_steps/) for basic setup instructions.

Similar to other supported reporters, the following attributes are required to enable pushgateway reporters:

```scala
hoodie.metrics.on=true
hoodie.metrics.reporter.type=PROMETHEUS_PUSHGATEWAY
```

The following properties are used to configure the address and port number of pushgateway. The default address is
localhost, and the default port is 9091

```scala
hoodie.metrics.pushgateway.host=xxxx
hoodie.metrics.pushgateway.port=9091
```

You can configure whether to delete the monitoring information from pushgateway at the end of the task, the default is true

```scala
hoodie.metrics.pushgateway.delete.on.shutdown=false
```

You can configure the task name prefix and whether a random suffix is required. The default is true

```scala
hoodie.metrics.pushgateway.job.name=xxxx
hoodie.metrics.pushgateway.random.job.name.suffix=false
```

### AWS CloudWatchReporter
Hudi supports publishing metrics to Amazon CloudWatch. It can be configured by setting [`hoodie.metrics.reporter.type`](https://hudi.apache.org/docs/next/configurations#hoodiemetricsreportertype) 
to “CLOUDWATCH”. Static AWS credentials to be used can be configured using 
[`hoodie.aws.access.key`](https://hudi.apache.org/docs/next/configurations#hoodieawsaccesskey),
[`hoodie.aws.secret.key`](https://hudi.apache.org/docs/next/configurations#hoodieawssecretkey),
[`hoodie.aws.session.token`](https://hudi.apache.org/docs/next/configurations#hoodieawssessiontoken) properties.
In the absence of static AWS credentials being configured, `DefaultAWSCredentialsProviderChain` will be used to get 
credentials by checking environment properties. Additional Amazon CloudWatch reporter specific properties that can be 
tuned are in the `HoodieMetricsCloudWatchConfig` class.

### UserDefinedMetricsReporter

Allows users to define a custom metrics reporter.

#### Configurations
The following is an example of `UserDefinedMetricsReporter`. More detailed configurations can be referenced [here](configurations#Metrics-Configurations).

```properties
hoodie.metrics.on=true
hoodie.metrics.reporter.class=test.TestUserDefinedMetricsReporter
```

#### Demo
In this simple demo, TestMetricsReporter will print all gauges every 10 seconds

```java
public static class TestUserDefinedMetricsReporter 
    extends AbstractUserDefinedMetricsReporter {
  private static final Logger log = LogManager.getLogger(DummyMetricsReporter.class);

  private ScheduledExecutorService exec = Executors.newScheduledThreadPool(1, r -> {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setDaemon(true);
      return t;
  });

  public TestUserDefinedMetricsReporter(Properties props, MetricRegistry registry) {
    super(props, registry);
  }

  @Override
  public void start() {
    exec.schedule(this::report, 10, TimeUnit.SECONDS);
  }

  @Override
  public void report() {
    this.getRegistry().getGauges().forEach((key, value) -> 
      log.info("key: " + key + " value: " + value.getValue().toString()));
  }

  @Override
  public Closeable getReporter() {
    return null;
  }

  @Override
  public void stop() {
    exec.shutdown();
  }
}
```

## HoodieMetrics

Once the Hudi writer is configured with the right table and environment for `HoodieMetrics`, it produces the following `HoodieMetrics`, that aid in debugging hudi tables

 - **Commit Duration** - The amount of time it took to successfully commit a batch of records
 - **Rollback Duration** - Similarly, the amount of time taken to undo partial data left over by a failed commit (rollback happens automatically after a failing write)
 - **File Level metrics** - Shows the amount of new files added, versions, deleted (cleaned) in each commit
 - **Record Level Metrics** - Total records inserted/updated etc per commit
 - **Partition Level metrics** - number of partitions upserted (super useful to understand sudden spikes in commit duration)

These `HoodieMetrics` can then be plotted on a standard tool like grafana. Below is a sample commit duration chart.

<figure>
    <img className="docimage" src={require("/assets/images/hudi_commit_duration.png").default} alt="hudi_commit_duration.png"  />
</figure>

## List of metrics:

1. totalPartitionsWritten
2. totalFilesInsert
3. totalFilesUpdate
4. totalRecordsWritten
5. totalUpdateRecordsWritten
6. totalInsertRecordsWritten
8. totalBytesWritten
8. totalScanTime
9. totalCreateTime
10. totalUpsertTime
11. totalCompactedRecordsUpdated
12. totalLogFilesCompacted
13. totalLogFilesSize
14. commitTime (Epoch in milliseconds)
15. duration (Milliseconds)

_Note:_ These metrics are available for all the timeline operations
