# hudi-trino-plugin

Hudi connector for Trino (RFC-105). Published as `org.apache.hudi:hudi-trino` -- a regular non-shaded JAR. The Trino-side `trino-hudi` plugin module depends on this artifact and Trino's URLClassLoader isolates the plugin's transitive deps from the rest of the server, so no shading is required.

The source directory is named `hudi-trino-plugin/`; the Maven artifactId is `hudi-trino`.

## Build

Excluded from default builds. Activate the `hudi-trino` Maven profile:

```
mvn -Phudi-trino -pl hudi-trino-plugin install
```

Requires JDK 25 (enforced via `maven-enforcer-plugin`).

## IDE setup

Only this module needs JDK 25. Leave the rest of Hudi on its native JDK (11 or 17) so you are not toggling the project default.

1. Activate the `hudi-trino` Maven profile so the IDE picks up the module.
   - IntelliJ: Maven tool window, Profiles, tick `hudi-trino`.
2. Override the SDK for the `hudi-trino-plugin` module only, to Temurin 25 with Language level 25.
   - IntelliJ: `File > Project Structure > Modules > hudi-trino-plugin > Dependencies > Module SDK`.

The enforcer rule only runs during `mvn`, not during the IDE's incremental compile.
