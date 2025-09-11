# Pull Request Title Guidelines

This project follows [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for PR titles to ensure consistency and enable automated tooling.

## Format

```
<type>(optional scope): <description>
```

For breaking changes that require attention, use

```
<type>(optional scope)!: <description>
```

## Types

The following commit types are allowed:

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation changes only
- **style**: Code style, formatting, or lint-only changes (no logic impact)
- **refactor**: Code changes that neither fix a bug nor add a feature (e.g., cleanups, restructuring)
- **perf**: Performance improvements
- **test**: Adding or correcting tests
- **chore**: Tooling, build system, CI/CD, or maintenance tasks
- **improvement**: Enhancements to existing functionality (aligns with past JIRA usage)
- **blocker**: Critical issues that block a release (rare, reserved use)
- **security**: Security-related fixes or improvements

## Examples

### Good PR Titles ✅

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

### Bad PR Titles ❌

```
Add new feature                   # Missing type
FIX: bug in login                 # Type should be lowercase
feat add authentication           # Missing colon
feature: new login system         # Invalid type (should be 'feat')
fix                               # Missing description
```

## Scope (Optional)

Scopes provide additional context and can be used to specify which part of the codebase is affected:

```
feat(flink): add bucket index implementation on Flink
fix(index): fix index update logic
```

## Breaking Changes

For breaking changes, add an exclamation mark after the type/scope:

```
feat!: change merger API to account for better delete handling
feat(index)!: change secondary index layout
```

## Validation

PR titles are automatically validated using GitHub Action of semantic validation.

If your PR title doesn't follow these guidelines, the validation check will fail and you'll need to update it before merging.

## Ignoring Validation

In rare cases, you can skip validation by adding one of these labels to your PR:
- `bot`
- `ignore-semantic-pull-request`

Or include `[skip ci]` in your PR title for CI-related changes.

## Resources

- [Conventional Commits Specification](https://www.conventionalcommits.org/en/v1.0.0/)
