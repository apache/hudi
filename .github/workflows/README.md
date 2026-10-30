## How to update the Pull Request Template

When updating the pr template, you must consider if updates need to be made to scripts/pr_compliance.py

## What are the files in workflows?
- bot.yml: Java CI. Spark lanes (JDK 11 on Spark 3.5, JDK 17 on Spark 4.2). Runs on PRs, merge-queue groups and pushes to master, release-* and branch-0.x; only push runs are path-filtered. No job depends on another, except the final `Java CI result` job, which aggregates the rest and reports on every PR and merge-queue run, so it can be required once the merge queue is enabled in .asf.yaml. Dropped lanes are commented in place and marked `[CI-TRIM]`
- java_ci_engines.yml: Java CI Engines. Flink, bundle validation and integration tests. Same triggers and layout rules as bot.yml; its aggregate job is `Java CI Engines result`. The two are separate workflows because the ASF GitHub Actions policy caps the jobs running at once in one workflow at 20, and a single workflow for both would exceed it
- validate_source.yml: PR and Source Validation. One job, `validate-pr-and-source`, the only required status check: it checks the PR description with scripts/pr_compliance.py, runs that script's own tests, validates the PR title, rejects commits with a Claude co-author trailer, and runs the source release checks (RAT, binary files, copyright). Never path-filtered, because a required check must report on every PR. Also runs on merge-queue groups, where only the source release checks apply
