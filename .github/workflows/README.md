## How to update the Pull Request Template

When updating the pr template, you must consider if updates need to be made to scripts/pr_compliance.py

## What are the files in workflows?
- bot.yml: runs the hudi unit tests against the scala, spark, and flink versions in its CI matrix. That matrix is currently cut down to one configuration per area to reduce runner usage; the jobs and matrix entries it dropped are commented out in place and marked `[CI-TRIM]`
- pr_validation.yml: one job (`validate-pr`) that checks the PR description with scripts/pr_compliance.py, runs that script's own tests, validates the PR title, and rejects commits with a Claude co-author trailer
