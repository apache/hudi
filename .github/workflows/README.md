## How to update the Pull Request Template

When updating the pr template, you must consider if updates need to be made to scripts/pr_compliance.py

## What are the files in workflows?
- ci.yml: CI. The top-level Java CI workflow: it owns the triggers (PRs, merge-queue groups, pushes to master, release-* and branch-0.x, manual dispatch) and the concurrency groups, and calls bot.yml and java_ci_engines.yml. Only push runs are path-filtered. Its `Required Checks` job aggregates both called workflows and reports on every PR and merge-queue run, so it can be required once the merge queue is enabled in .asf.yaml
- bot.yml: Java CI. Called by ci.yml. Spark lanes (JDK 11 on Spark 3.5, JDK 17 on Spark 4.2). No job depends on another. Dropped lanes are commented in place and marked `[CI-TRIM]`
- java_ci_engines.yml: Java CI Engines. Called by ci.yml. Flink, bundle validation and integration tests. Same layout rules as bot.yml
- validate_source.yml: PR and Source Validation. One job, `validate-pr-and-source`, the only required status check: it checks the PR description with scripts/pr_compliance.py, runs that script's own tests, validates the PR title, rejects commits with a Claude co-author trailer, and runs the source release checks (RAT, binary files, copyright). Never path-filtered, because a required check must report on every PR. Also runs on merge-queue groups, where only the source release checks apply
