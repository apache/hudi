---
title: "Monitor Hudi metrics with Datadog"
excerpt: "Introducing the feature of reporting Hudi metrics via Datadog HTTP API"
authors: [xushiyan]
category: blog
tags:
- how-to
- metrics
- apache hudi
---

## Availability

**0.6.0 (unreleased)**

## Introduction

[Datadog](https://www.datadoghq.com/) is a popular monitoring service. In the upcoming `0.6.0` release of Apache Hudi, we will introduce the feature of reporting Hudi metrics via Datadog HTTP API, in addition to the current reporter types: Graphite and JMX.
<!--truncate-->
## Configurations

Similar to other supported reporters, turning on Datadog reporter requires these 2 properties.

```properties
hoodie.metrics.on=true
hoodie.metrics.reporter.type=DATADOG
```

The following property sets the Datadog API site. It determines whether the requests will be sent to `api.datadoghq.eu` (EU) or `api.datadoghq.com` (US). Set this according to your Datadog account settings.

```properties
hoodie.metrics.datadog.api.site=EU # or US
```

The property `hoodie.metrics.datadog.api.key` allows you to set the api key directly from the configuration. 

```properties
hoodie.metrics.datadog.api.key=<your api key>
hoodie.metrics.datadog.api.key.supplier=<your api key supplier>
```

Due to security consideration in some cases, you may prefer to return the api key at runtime. To go with this approach, implement `java.util.function.Supplier<String>` and set the implementation's FQCN to `hoodie.metrics.datadog.api.key.supplier`, and make sure `hoodie.metrics.datadog.api.key` is _not_ set since it will take higher precedence.

The following property helps segregate metrics by setting different prefixes for different jobs. 

```properties
hoodie.metrics.datadog.metric.prefix=<your metrics prefix>
```

Note that it will use `.` to delimit the prefix and the metric name. For example, if the prefix is set to `foo`, then `foo.` will be prepended to the metric name.

There are other optional properties, which are explained in the configuration reference page.

## Demo

In this demo, we ran a `HoodieDeltaStreamer` job with metrics turn on and other configurations set properly. 

![datadog metrics demo](/assets/images/blog/2020-05-28-datadog-metrics-demo.png)

As shown above, we were able to collect Hudi's action-related metrics like

- `<prefix>.<table name>.commit.totalScanTime`
- `<prefix>.<table name>.clean.duration`
- `<prefix>.<table name>.index.lookup.duration`

as well as `HoodieDeltaStreamer`-specific metrics

- `<prefix>.<table name>.deltastreamer.duration`
- `<prefix>.<table name>.deltastreamer.hiveSyncDuration`
