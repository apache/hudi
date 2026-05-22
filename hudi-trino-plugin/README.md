# hudi-trino-plugin

Hudi connector for Trino (RFC-105). Published as part of `org.apache.hudi:hudi-trino-bundle`; the Trino-side `HudiPlugin` shim loads classes from that bundle.

## Build

Excluded from default builds. Activate the `hudi-trino` Maven profile:

```
mvn -Phudi-trino -pl hudi-trino-plugin,packaging/hudi-trino-bundle -am install
```

Requires JDK 25 (enforced via `maven-enforcer-plugin`).

## IDE setup

Only `hudi-trino-plugin` needs JDK 25. Leave the rest of Hudi on its native JDK (11 or 17) so you are not toggling the project default.

1. Activate the `hudi-trino` Maven profile so the IDE picks up the module.
   - IntelliJ: Maven tool window, Profiles, tick `hudi-trino`.
2. Override the SDK for the `hudi-trino-plugin` module only, to Temurin 25 with Language level 25.
   - IntelliJ: `File > Project Structure > Modules > hudi-trino-plugin > Dependencies > Module SDK`.

The enforcer rule only runs during `mvn`, not during the IDE's incremental compile.