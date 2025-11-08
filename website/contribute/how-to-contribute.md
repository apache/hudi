---
sidebar_position: 2
title: "How to Contribute"
toc: true
last_modified_at: 2020-09-01T15:59:57-04:00
---

Apache Hudi community welcomes contributions from anyone!

## Ways to become a contributor

A GitHub account is needed to file issues, start discussions, and send pull requests to Hudi. Here are a few ways you can get involved.

 - Engage with the community on [GitHub Discussions](https://github.com/apache/hudi/discussions) or Slack
 - Help improve docs and contribute blogs [here](https://github.com/apache/hudi/tree/asf-site) for hudi.apache.org
 - Share [new feature](https://github.com/apache/hudi/issues/new?template=hudi_feature.yml) requests or propose a [new RFC](/contribute/rfc-process)
 - Contribute code to the project by raising [pull requests (PR)](https://github.com/apache/hudi/pulls) adhering to the [contribution guide](/contribute/developer-setup). Here are some good [first issues](https://github.com/apache/hudi/issues?q=state%3Aopen%20label%3Agood-first-issues).
 - Report [bugs](https://github.com/apache/hudi/issues/new?template=hudi_bug.yml) or suggest [improvements](https://github.com/apache/hudi/issues/new?template=hudi_improvement.yml) to the user experience; review code or RFCs on GitHub
 - Share your success story on [Hudi LinkedIn](https://www.linkedin.com/company/apache-hudi/) Community Syncs.
 - Pull requests can only be merged by a Hudi committer, listed [here](/community/team), but anyone is free to review.
 - [Voting on a release](https://www.apache.org/foundation/voting): Everyone can vote on the dev mailing list. Only Hudi PMC members should mark their votes as binding.

All communication is expected to align with the [Code of Conduct](https://www.apache.org/foundation/policies/conduct).

## Contributing on GitHub

:::note Developer setup 
 If you are planning to contribute code, please refer to [developer setup](/contribute/developer-setup) for instructions and information that will 
 help you get going.
:::

This document details the processes and procedures we follow to make contributions to the project.
If you are looking to ramp up as a contributor to the project, we highly encourage you to read this guide in full and familiarize yourself with the workflow.

## Filing Issues

Hudi manages development tasks and project/release management using GitHub Issues, following the process and protocol below.

There are five types of GitHub Issues.

| Issue type        | Purpose                                                 | Who can file                           | Label                  |
|:------------------|:--------------------------------------------------------|----------------------------------------|------------------------|
| Epic              | Roadmap tracking across multiple releases               | Only created by maintainers/committers | type:epic              |
| Feature           | New feature development stories. Can have sub-issues    | Anyone                                 | type:feature           |
| Improvements      | Regular dev tasks and improvements. Can have sub-issues | Anyone                                 | type:devtask           |
| Bug               | For issues that are fixing bugs in Hudi                 | Anyone                                 | type:bug               |
| Community Support | Problems reported that may still need triaging          | Anyone                                 | type:community-support |

When filing issues, please follow the issue templates tightly to ensure smooth project management for everyone involved. 

Some things to keep in mind and strive for: 

- Make an attempt to find an existing issue that may solve the same issue you are reporting.
- Carefully gauge whether a new feature needs an [RFC](/contribute/rfc-process).
- If you intend to target an issue for a specific release, please mark the release using the `milestone` field on the GitHub issue.
- If you are not sure, please wait for a PMC/Committer to confirm/triage the issue and accept it. This also avoids contributors spending time on issues with unclear scope.
- Whenever possible, break down large issues into sub-issues such that each sub-issue can be fixed by a PR of reasonable size/complexity.
- You can also contribute by helping others contribute. So, if you don't have cycles to work on an issue and another contributor offers help, take it!

When in doubt, you can always start a GitHub discussion so that the community can provide early feedback and point out any similar issues, PRs, or RFCs.

## Opening Pull Requests

This project follows [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for PR titles to ensure consistency and enable automated tooling.

:::important
All pull requests must either reference a GitHub Issue or describe the issue inline clearly within the pull request.
Refer to an issue using `closes #<issue_number>` if intending to auto-close the issue when the pull request is merged or closed,
or simply `issue: #<issue_number>` if auto-close is not desirable.

For larger features, maintainers may insist on filing GitHub Issues, appropriately linked to other GitHub issues.
:::

### PR Title Format

```
<type>(optional scope): <description>
```

For breaking changes that require attention, use

```
<type>(optional scope)!: <description>
```

The following types are allowed.

| type     | Purpose                                                                                |
|:---------|:---------------------------------------------------------------------------------------|
| feat     | new feature addition                                                                   |
| fix      | bug fix                                                                                |
| docs     | doc changes only - code or html or .md files                                           |
| style    | Code style, formatting or lint-only changes with 0 logic changes                       |
| refactor | Code changes that are neither fixing or adding features. cleanup, redoing abstractions |
| perf     | Performance improvements or tooling                                                    |
| test     | Adding, fixing tests and test infrastructure.                                          |
| chore    | Tooling, build system, CI/CD, or maintenance tasks                                     |

#### Scopes
Optionally, any of the below can be added as scope to PRs. Scopes provide additional context and can be used to specify which part of the codebase is affected.
This helps us track where development activity is directed and whether bugs on a component are being resolved in a timely fashion. Tooling should auto-apply the 
right label to pull requests and issues.

| Scope          | Purpose                                                                         | Label               |
|:---------------|:--------------------------------------------------------------------------------|---------------------|
| common         | common code or abstractions shared across the entire project                    | area:common         |
| core           | Changes affecting transaction management, concurrency and core read/write flows | area:core           |
| api            | Any changes affecting public apis or interfaces                                 | area:api            |
| config         | Any changes affecting public configs                                            | area:config         |
| storage-format | Any changes to bits on storage - timeline, index, data and metadata             | area:storage-format |
| metadata-table | Changes around metadata table                                                   | area:metadata-table |
| table-services | Cleaning, Clustering, Compaction, Log Compaction, Indexing, TTL, ...            | area:table-services |
| tools          | Any tools like CLI                                                               | area:tools          |
| ingest         | Spark and Flink streamer tools, to ELT data into Hudi. Kafka sink               | area:ingest         |
| spark          | Spark SQL, Streaming, Structured Streaming, Data source                         | engine:spark        |
| flink          | DataStream writing/reading, SQL, Dynamic Tables                                 | engine:flink        |
| trino          | Trino Hudi connector maintained in Hudi repo                                    | engine:trino        |


For example:

```
feat(flink): add bucket index implementation on Flink
fix(index): fix index update logic
```

When broken down adequately, most issues and pull requests should address just one primary area or scope respectively.
But, there may be some special situations.

1. If your PR makes any API, core, or storage-format changes, it absolutely must be called out.
2. If you are unsure about the component to use either because the PR or issue goes across them or it falls outside the list above, omit the scope in the PR title or issue label.

If your PR is targeting an old JIRA (before Hudi migrated to GitHub Issues), put the JIRA number in the scope.

```
feat(HUDI-1234): add a new feature
```

### Examples

#### Good PR Titles ✅

```
feat: add a new index in the metadata table
fix: resolve type handling in data processing
docs: update installation instructions
style: format code according to style guide
refactor: extract common utility functions
perf: optimize Spark query performance
test: add unit tests for common storage layout
chore: resolve checkstyle warnings
improvement: enhance error handling in type cast
blocker: fix class loading failure on Java 17
security: update dependencies to latest versions
```

#### Bad PR Titles ❌

```
Add new feature                   # Missing type
FIX: bug in login                 # Type should be lowercase
feat add authentication           # Missing colon
feature: new login system         # Invalid type (should be 'feat')
fix                               # Missing description
```

#### Breaking Changes

For breaking changes, add an exclamation mark after the type/scope:

```
feat!: change merger API to account for better delete handling
feat(index)!: change secondary index layout
```
PRs with breaking changes will be subject to broader reviews and opinions before they are merged.

#### Validation

PR titles are automatically validated using GitHub Actions for semantic validation.

If your PR title doesn't follow these guidelines, the validation check will fail and you'll need to update it before merging.

In rare cases, you can skip validation by adding one of these labels to your PR:
- `bot`
- `ignore-semantic-pull-request`

Or include `[skip ci]` in your PR title for CI-related changes.

## Coding guidelines

Our code can benefit from contributors speaking the same "language" when authoring code. After all, it gets read a lot more than it gets
written, so optimizing for "reads" is a good goal. The list below is a set of guidelines that contributors strive to uphold and is reflective
of how we want to evolve our code in the future.

### Style

- **Formatting** We should rely on checkstyle and spotless to auto-fix formatting; automate this completely. Where we cannot,
  we will err on the side of not taxing contributors with manual effort.
- **Refactoring**
    - Refactor with purpose; any refactor suggested should be attributable to functionality that now becomes easy to implement.
    - A class is asking to be refactored when it has several overloaded responsibilities or has sets of fields/methods that are used more cohesively than others.
    - Try to name tests using the given-when-then model, which cleanly separates preconditions (given), an action (when), and assertions (then).
- **Naming things**
    - Let's name uniformly by using the same word to denote the same concept. For example: bootstrap vs external vs source, when referring to bootstrapped tables.
      Maybe they all mean the same, but having one word makes the code a lot more easily readable.
    - Let's name consistently with Hudi terminology. For example: dataset vs table, base file vs data file.
    - Class names preferably are nouns (e.g Runner) which reflect their responsibility and methods are verbs (e.g run()).
    - Avoid filler words that don't add value, for example: xxxInfo, xxxData, etc.
    - We name classes in code starting with `Hoodie` and not `Hudi` and we want to keep it that way for consistency/historical reasons.
- **Methods**
    - Individual methods should be short (~20-30 lines) and have a single purpose. If you feel like it has a secondary purpose, then maybe it needs
      to be broken down more.
    - The fewer the number of arguments, the better.
    - Place caller methods on top of callee methods, whenever possible.
    - Avoid "output" arguments, for example: passing in a list and filling its values within the method.
    - Try to limit individual if/else blocks to few lines to aid readability.
    - Separate logical blocks of code with a newline in between, for example: read a file into memory, loop over the lines.
- **Classes**
    - Like methods, each class should have a single purpose/responsibility.
    - Try to keep class files to about 200 lines of length, nothing beyond 500.
    - Avoid stating the obvious in comments; for example, each line does not deserve a comment. Document corner cases/special performance considerations, etc., clearly.
    - Try creating factory methods/builders and interfaces wherever you feel a specific implementation may be changed down the line.

#### Substance

Try to avoid large PRs; if unavoidable (many times they are), please separate refactoring from the actual implementation of functionality.
For example, renaming/breaking up a file and then making code changes makes the diff very hard to review.
- **Licensing**
    - Every source file needs to include the Apache license header. Every new dependency needs to have
      an open source license [compatible](https://www.apache.org/legal/resolved#criteria) with Apache.
    - If you are reusing code from another Apache/open-source project, licensing needs to be compatible and attribution added to the `LICENSE` file
    - Please DO NOT copy-paste any code from StackOverflow or other online sources, since their license attribution would be unclear. Author them yourself!
- **Code Organization**
    - Anything in `hudi-common` cannot depend on a specific engine runtime like Spark.
    - Any changes to bundles under `packaging`, will be reviewed with additional scrutiny to avoid breakages across versions.
- **Code reuse**
    - Whenever you can, please use/enhance use existing utils classes in code (`CollectionUtils`, `ParquetUtils`, `HoodieAvroUtils`). Search for classes ending in `Utils`.
    - As a complex project that must integrate with multiple systems, we tend to avoid dependencies like `guava` and `apache commons` for the sake of easy integration.
      Please start a discussion on the mailing list before attempting to reintroduce them
    - As a data system that takes performance seriously, we also write pieces of infrastructure (e.g., `ExternalSpillableMap`) natively that are optimized specifically for our scenarios.
      Please start with them first when solving problems.
- **Breaking changes**
    - Any version changes for dependencies need to be ideally vetted across different user environments in the community to get enough confidence before merging.
    - Any changes to methods annotated with `PublicAPIMethod` or classes annotated with `PublicAPIClass` require upfront discussion and potentially an RFC.
    - Any non-backwards compatible changes similarly need upfront discussion and the functionality needs to implement an upgrade-downgrade path.
- **Documentation**
   - Where necessary, please ensure there is another PR to [update the docs](https://github.com/apache/hudi/tree/asf-site/README.md) as well.
   - Keep RFCs up to date as implementation evolves.

### Testing 
Add adequate tests for your new functionality. For involved changes, it's best to test the changes in real production environments and report the results in the PR.
For website changes, please build the site locally & test navigation, formatting & links thoroughly

- **Categories**
    - unit - testing basic functionality at the class level, potentially using mocks. Expected to finish quicker
    - functional - brings up the services needed and runs test without mocking
    - integration - runs a subset of functional tests on a full-fledged environment with dockerized services
- **Prepare Test Data**
    - Many unit and functional test cases require a Hudi dataset to be prepared beforehand. `HoodieTestTable` and `HoodieWriteableTestTable` are dedicated test utility classes for this purpose. Use them whenever appropriate, and add new APIs to them when needed.
    - When adding new APIs in the test utility classes, overload APIs with a variety of arguments to do more heavy-lifting for callers.
    - In most scenarios, you won't need to use `FileCreateUtils` directly.
    - If test cases require interaction with actual `HoodieRecord`s, use `HoodieWriteableTestTable` (and `HoodieTestDataGenerator` probably). Otherwise, `HoodieTestTable` that manipulates empty files shall serve the purpose.
- **Strive for Readability**
    - Avoid writing flow controls for different assertion cases. Split to a new test case when appropriate.
    - Use plain for-loop to avoid try-catch in lambda block. Declare exceptions is okay.
    - Use static import for constants and static helper methods to avoid lengthy code.
    - Avoid reusing local variable names. Create new variables generously.
    - Keep helper methods local to the test class until it becomes obviously generic and reusable. When that happens, move the helper method to the right utility class. For example, `Assertions` contains common assert helpers, and `SchemaTestUtil` is for schema-related helpers.
    - Avoid putting new helpers in `HoodieTestUtils` and `HoodieClientTestUtils`, which are named too generic. Eventually, all test helpers shall be categorized properly.

## Reviewing Pull Requests/RFCs

- All pull requests would be subject to code reviews, from one or more of the PMC/Committers.
- Typically, each PR will get an "Assignee" based on their area of expertise, who will work with you to land the PR.
- Code reviews are vital, but also often time-consuming for everyone involved. Below are some principles which could help align us better.
    - Reviewers need to provide actionable, concrete feedback that states what needs to be done to get the PR closer to landing.
    - Reviewers need to make it explicit, which of the requested changes would block the PR vs good-to-dos.
    - Both contributors/reviewers need to keep an open mind and ground themselves to making the most technically sound argument.
    - If progress is hard, please involve another PMC member/Committer to share another perspective.
    - Staying humble and eager to learn, goes a long way in ensuring these reviews are smooth.
- Reviewers are expected to uphold the code quality, standards outlined above.
- When merging PRs, always make sure you are squashing the commits using the "Squash and Merge" feature in GitHub
- When necessary/appropriate, reviewers could make changes themselves to PR branches, with the intent to get the PR landed sooner.
  Reviewers should seek explicit approval from the author before making large changes to the original PR.

### Proposing Changes
We welcome new ideas and suggestions to improve the project along any dimensions - management, processes, technical vision/direction. To kick-start a discussion on the mailing thread
to effect change and source feedback, start a new email thread with the `[DISCUSS]` prefix and share your thoughts. If your proposal leads to a larger change, then it may be followed up
by a [vote](https://www.apache.org/foundation/voting) by a PMC member or others (depending on the specific scenario). For technical suggestions, you can also leverage [our RFC Process](/contribute/rfc-process) to outline your ideas in greater detail.

## Becoming a Committer

We are always looking for strong contributors, who can become [committers](https://www.apache.org/dev/committers) on the project.
Committers are chosen by a majority vote of the Apache Hudi [PMC](https://www.apache.org/foundation/how-it-works#pmc-members), after a discussion on their candidacy based on the following criteria (not exclusive/comprehensive).

- Embodies the ASF model code of [conduct](https://www.apache.org/foundation/policies/conduct)
- Has made significant technical contributions such as submitting PRs, filing bugs, testing, benchmarking, authoring RFCs, providing feedback/code reviews (+ more).
- Has helped the community over a few months by answering questions on support channels above and triaging issues.
- Demonstrates clear code/design ownership of a component or code area (e.g., Hudi Streamer, Hive/Presto Integration, etc.).
- Brought thought leadership and new ideas into the project and evangelized them with the community via conference talks, blog posts.
- Great citizenship in helping with all peripheral (but very critical) work like site maintenance, wiki cleanups, and so on.
- Proven commitment to the project by way of upholding all agreed upon processes, conventions and principles of the community.
