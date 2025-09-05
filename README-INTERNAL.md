# Hudi Internal Development Guide

This document contains internal development notes and commands for working with the Hudi codebase.

Before building, run the authentication command:

```bash
code_auth
```
## Code Quality Checks

### Checkstyle

Run checkstyle:

```bash
# Check entire project
mvn checkstyle:check

# Check specific module
mvn checkstyle:check -pl hudi-sync/hudi-datahub-sync
```

## Build Commands

### Clean Build
```bash
mvn -T 14C clean package -DskipTests -Dscala-2.12 -Dspark3.5 -Dflink1.20 
```