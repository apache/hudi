## How to update the Pull Request Template

When updating the pr template, you must consider if updates need to be made to scripts/pr_compliance.py

## What are the files in workflows?
- bot.yml: runs the hudi unit tests with various versions of scala, spark, and flink
- pr_compliance.yml: checks pr titles and main comment to make sure that everything is filled out and formatted properly
- update_pr_compliance: runs the pr_compliance tests when scripts/pr_compliance.py is updated


