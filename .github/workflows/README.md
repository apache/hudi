## How to update the Pull Request Template

When updating the pr template, you must consider if updates need to be made to scripts/pr_compliance.py

## What are the files in workflows?
- ci.yml: CI. The top-level Java CI workflow: it owns the triggers (PRs, merge-queue groups, branch pushes, manual dispatch) and the concurrency groups, and calls bot.yml and java_ci_engines.yml. Only push runs are path-filtered. Forks run it on every branch push; upstream it skips pushes to branches other than master, release-* and branch-0.x, and skips both called workflows on PRs from forks to master (see [Pull requests from forks](#pull-requests-from-forks)). Its `Required Checks` job aggregates both called workflows and reports on every PR and merge-queue run, so it can be required once the merge queue is enabled in .asf.yaml
- fork_ci_status.yml: Fork CI status. Mirrors the fork's ci.yml run for the head commit of each open fork PR to master onto the PR as the informational `CI (fork)` check. Runs when an upstream CI run completes and every 15 minutes for late runs and reruns. It never checks out PR code. Its tests, .github/ci/scripts/test-fork-ci-status.cjs, also cover the ci.yml routing and concurrency
- bot.yml: Java CI. Called by ci.yml. Spark lanes (JDK 11 on Spark 3.5, JDK 17 on Spark 4.2). No job depends on another. Dropped lanes are commented in place and marked `[CI-TRIM]`
- java_ci_engines.yml: Java CI Engines. Called by ci.yml. Flink, bundle validation and integration tests. Same layout rules as bot.yml
- validate_source.yml: PR and Source Validation. One job, `validate-pr-and-source`, the only required status check: it checks the PR description with scripts/pr_compliance.py, runs that script's own tests and the fork_ci_status.yml tests, validates the PR title, rejects commits with a Claude co-author trailer, and runs the source release checks (RAT, binary files, copyright). Never path-filtered, because a required check must report on every PR. Also runs on merge-queue groups, where only the source release checks apply

## Pull requests from forks

To reduce load on ASF's shared runners, pull requests from forks to `master` run the Java CI suites on your fork using your fork's GitHub runners, with lightweight checks upstream. To enable Actions in your fork:

1. Open the Actions tab in your fork and enable workflows before pushing.
2. Sync your fork and rebase your branch onto upstream `master` to include the current CI workflows.
3. Push your branch, or manually run CI from your fork's Actions tab.

View results and rerun failed jobs there; the PR's `CI (fork)` check may lag. A push that changes only docs or images creates no fork run, because ci.yml path-filters push events.

The `CI (fork)` check links to the fork run and is informational. When your PR is approved and added to the merge queue, the full Java CI runs using ASF's runners against the proposed merge with `master`. Release-branch PRs, and PRs from branches pushed to apache/hudi itself, keep upstream testing.
