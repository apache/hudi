# hudi-trino-bundle

Shaded jar that ships the Trino-Hudi connector. Published as `org.apache.hudi:hudi-trino-bundle` (RFC-105). The Trino-side `HudiPlugin` shim depends on this artifact and loads `io.trino.plugin.hudi.HudiConnectorFactory` from it.

## What is inside

Connector classes (unshaded, canonical FQN preserved):
- `io.trino.plugin.hudi.*` from `hudi-trino-plugin`

Hudi runtime jars (no relocation):
- `hudi-common`, `hudi-client-common`, `hudi-java-client`, `hudi-hadoop-common`, `hudi-hadoop-mr`

Shaded third-party libs (relocated under `org.apache.hudi.*` or the `trino.bundle.bootstrap.shade.prefix`):
- Kryo, parquet-avro, avro, caffeine, commons-io, dropwizard metrics, jol-core, protobuf

What is NOT inside:
- `META-INF/services/io.trino.spi.Plugin`. That manifest lives only on the Trino-side `trino-hudi` shim; never ship it from this bundle.

## Build

Two-step because `hudi-common` etc. use Lombok 1.18.36 which is not compatible with JDK 25, while `hudi-trino-plugin` requires JDK 25 for the Trino SPI. See `hudi-trino-plugin/README.md` for the why.

```
# Step 1, on JDK 17: install Hudi base modules into ~/.m2
JAVA_HOME=$(/usr/libexec/java_home -v 17) \
  mvn install -DskipTests -pl hudi-common,hudi-io,hudi-hive-sync,hudi-sync-common,hudi-hadoop-common,hudi-client-common,hudi-java-client,packaging/hudi-hadoop-mr-bundle -am

# Step 2, on JDK 25: build the connector and bundle
JAVA_HOME=$(/usr/libexec/java_home -v 25) \
  mvn install -Phudi-trino -pl hudi-trino-plugin,packaging/hudi-trino-bundle
```

The shaded jar lands at `packaging/hudi-trino-bundle/target/hudi-trino-bundle-1.3.0-SNAPSHOT.jar` and is also installed to `~/.m2/repository/org/apache/hudi/hudi-trino-bundle/`.

## Verify the jar

```
JAR=packaging/hudi-trino-bundle/target/hudi-trino-bundle-1.3.0-SNAPSHOT.jar

# Connector entrypoint must be present at its canonical FQN
jar tf "$JAR" | grep -E 'io/trino/plugin/hudi/HudiConnectorFactory\.class$'

# Plugin SPI manifest must NOT be present (only the Trino-side shim ships that)
jar tf "$JAR" | grep -E 'META-INF/services/io\.trino\.spi\.Plugin$' && echo "FAIL: bundle leaks Plugin SPI" || echo "OK"
```

## Consume from Trino

The Trino-side `plugin/trino-hudi` is already wired:
- `<dependency>org.apache.hudi:hudi-trino-bundle</dependency>` in `plugin/trino-hudi/pom.xml`.
- Version property `<dep.hudi-trino.version>` in the root Trino `pom.xml`. Bump this to point at a newer published bundle.

Local development flow:
```
# In the Trino repo, after step 2 above has installed the bundle to ~/.m2
./mvnw -pl :trino-hudi install
```

The Trino build resolves `hudi-trino-bundle` from your local `~/.m2`. For Hudi snapshots, ensure Apache's snapshot repo is enabled in your Maven settings; otherwise the version property must point at a released version.

## Publish

Follows the standard Hudi `mvn deploy` flow under the `hudi-trino` profile:

```
JAVA_HOME=$(/usr/libexec/java_home -v 25) \
  mvn deploy -Phudi-trino -pl hudi-trino-plugin,packaging/hudi-trino-bundle -DskipTests
```

For release candidates, include this bundle in the RC validation step alongside the other published Hudi bundles.