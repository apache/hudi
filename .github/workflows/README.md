## How to update the Pull Request Template

When updating the pr template, you must consider if updates need to be made to scripts/pr_compliance.py

## What are the files in workflows?
- bot.yml: Java CI. Spark lanes (JDK 11 on Spark 3.5, JDK 17 on Spark 4.2). Path-filtered at the trigger; no job depends on another. Dropped lanes are commented in place and marked `[CI-TRIM]`
- java_ci_engines.yml: Java CI Engines. Flink, bundle validation and integration tests. Same trigger filter and layout rules as bot.yml
- validate_source.yml: Validation. The two required status checks (validate-source, validate-ci-baseline). validate-source also checks the PR description with scripts/pr_compliance.py, runs that script's own tests, validates the PR title, and rejects commits with a Claude co-author trailer. Never path-filtered, because a required check must report on every PR
