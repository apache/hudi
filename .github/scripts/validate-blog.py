#!/usr/bin/env python3
"""
Validate blog post frontmatter for Apache Hudi website.

This script checks newly added or modified blog posts for:
1. Required fields (title, author/authors, category, image, tags)
2. Valid category values
3. Tag allowlist validation
4. Tag format rules
"""

import re
import sys
from difflib import get_close_matches
from pathlib import Path

# Allowed categories
ALLOWED_CATEGORIES = {'community', 'deep-dive', 'how-to', 'case-study'}

# Allowed tags - frozen set of validated tags
# To add a new tag: include it in your PR and ask the reviewer for approval
ALLOWED_TAGS = {
    'data lakehouse', 'aws', 'gcp', 'azure', 'apache spark', 'apache iceberg', 'indexing',
    'delta lake', 'incremental processing', 'beginner', 'onehouse', 'performance',
    'streaming', 'querying', 'comparison', 'hudi streamer', 'apache flink',
    'dml', 'concurrency control', 'cdc',
    'bi', 'uber', 'tutorial', 'release', 'cow', 'apache kafka', 'minio',
    'clustering', 'acid', 'walmart', 'python', 'mor', 'hudi timeline',
    'debezium', 'data skipping', 'daft', 'ai',
    'table format', 'starrocks', 'halodoc', 'gdpr', 'schema', 'scd',
    'observability', 'metadata', 'meetup', 'key generation', 'docker',
    'cleaner', 'apache hive', 'apache doris', 'vector search', 'upstox',
    'tla specification', 'streamlit', 'rag', 'presto', 'postgres',
    'monotonic timestamp', 'file sizing', 'etl', 'databricks', 'data warehouse',
    'conference', 'compaction', 'bootstrap', 'apache parquet', 'announcement',
    'zupee', 'yuno', 'yugabyte', 'yahoo', 'robinhood', 'peloton', 'leboncoin',
    'grofers', 'grab', 'funding circle', 'freewheel', 'estuary', 'alibaba',
    'airbyte', 'apache xtable', 'data catalog',
    'write', 'trino', 'terraform', 'table services', 'storage types', 'apache paimon',
    'storage spec', 'sql transformer', 'snapshot exporter', 'security',
    'risingwave', 'record mergers', 'ray', 'puppygraph', 'openai',
    'open architecture', 'mongodb', 'modern data architecture', 'mlops',
    'migration', 'markers', 'lsm tree', 'lock provider', 'late arriving data',
    'lakefs', 'interoperability', 'hudi cli', 'guide', 'google scholar',
    'forefathers', 'fastapi', 'dremio', 'deployment', 'deduplication', 'dbt',
    'database', 'data sharing', 'data processing', 'data platform', 'data mesh',
    'data governance', 'compression', 'commits', 'code sample', 'caching',
    'bytearray', 'best practices', 'backfilling', 'architecture',
    'apicurio registry', 'apache zeppelin', 'apache orc', 'apache dolphinscheduler',
    'apache avro', 'apache', 'access control',
}

# Tags that should not be used
FORBIDDEN_TAGS = {
    'apache hudi': 'Redundant - every blog on this site is about Apache Hudi',
    'hudi': 'Redundant - every blog on this site is about Apache Hudi',
    'index': 'Use "indexing" instead',
    'partition': 'Removed - too generic',
    'intermediate': 'Removed - use category "deep-dive" instead',
    'design': 'Removed - use category "deep-dive" instead',
    'community': 'Removed - use category "community" instead',
}


def suggest_similar_tags(tag: str, allowed: set, n: int = 3) -> list[str]:
    """Find similar tags from the allowed set."""
    # Use difflib for fuzzy matching
    matches = get_close_matches(tag.lower(), [t.lower() for t in allowed], n=n, cutoff=0.5)
    # Return original casing from allowed set
    result = []
    for match in matches:
        for allowed_tag in allowed:
            if allowed_tag.lower() == match:
                result.append(allowed_tag)
                break
    return result

# Required frontmatter fields
REQUIRED_FIELDS = ['title', 'category', 'image']


def parse_frontmatter(content: str) -> dict | None:
    """Extract frontmatter from markdown content."""
    match = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
    if not match:
        return None

    frontmatter = {}
    current_key = None
    current_list = None

    for line in match.group(1).split('\n'):
        # List item
        if line.startswith('- '):
            if current_list is not None:
                current_list.append(line[2:].strip())
            continue

        # Key-value pair
        if ':' in line:
            # Save previous list if any
            if current_key and current_list is not None:
                frontmatter[current_key] = current_list
                current_list = None

            key, _, value = line.partition(':')
            key = key.strip()
            value = value.strip()

            if value:
                # Remove quotes if present
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                frontmatter[key] = value
                current_key = None
            else:
                # This might be a list
                current_key = key
                current_list = []

    # Save final list if any
    if current_key and current_list is not None:
        frontmatter[current_key] = current_list

    return frontmatter


def validate_blog(filepath: str) -> list[str]:
    """Validate a single blog post. Returns list of error messages."""
    errors = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        return [f"Failed to read file: {e}"]

    frontmatter = parse_frontmatter(content)
    if frontmatter is None:
        return ["Missing or invalid frontmatter (must start with --- and end with ---)"]

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in frontmatter:
            # author or authors is acceptable
            if field == 'author' and 'authors' not in frontmatter:
                errors.append("Missing required field: 'author' or 'authors'")
        elif not frontmatter[field]:
            errors.append(f"Field '{field}' is empty")

    # Check for author/authors
    if 'author' not in frontmatter and 'authors' not in frontmatter:
        errors.append("Missing required field: 'author' or 'authors'")

    # Validate category
    category = frontmatter.get('category', '')
    if category and category not in ALLOWED_CATEGORIES:
        errors.append(
            f"Invalid category '{category}'. "
            f"Must be one of: {', '.join(sorted(ALLOWED_CATEGORIES))}"
        )

    # Validate tags (optional, but if present must follow format rules)
    tags = frontmatter.get('tags', [])
    if isinstance(tags, str):
        tags = [tags]

    for tag in tags:
        tag_lower = tag.lower().strip()

        # Check forbidden tags
        if tag_lower in FORBIDDEN_TAGS:
            errors.append(
                f"Forbidden tag '{tag}': {FORBIDDEN_TAGS[tag_lower]}"
            )
            continue

        # Check for hyphens in tags
        if '-' in tag:
            errors.append(
                f"Tag '{tag}' contains hyphen. "
                f"Use spaces instead (e.g., 'apache spark' not 'apache-spark')"
            )
            continue

        # Check against allowed tags
        if tag_lower not in {t.lower() for t in ALLOWED_TAGS}:
            suggestions = suggest_similar_tags(tag, ALLOWED_TAGS)
            if suggestions:
                errors.append(
                    f"Unknown tag '{tag}'. "
                    f"Did you mean: {', '.join(repr(s) for s in suggestions)}? "
                    f"If a new tag is needed, add it to ALLOWED_TAGS in .github/scripts/validate-blog.py "
                    f"and ask your PR reviewer for approval."
                )
            else:
                errors.append(
                    f"Unknown tag '{tag}'. "
                    f"Please use an existing tag from ALLOWED_TAGS in .github/scripts/validate-blog.py. "
                    f"If a new tag is needed, add it to the allowlist and ask your PR reviewer for approval."
                )

    return errors


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: validate-blog.py <file1> [file2] ...")
        print("       validate-blog.py --all")
        sys.exit(1)

    if sys.argv[1] == '--all':
        # Validate all blogs in the directory
        blog_dir = Path('website/blog')
        if not blog_dir.exists():
            print(f"Blog directory not found: {blog_dir}")
            sys.exit(1)
        files = list(blog_dir.glob('*.md')) + list(blog_dir.glob('*.mdx'))
    else:
        files = [Path(f) for f in sys.argv[1:]]

    total_errors = 0
    files_with_errors = 0

    for filepath in files:
        if not filepath.exists():
            print(f"⚠️  File not found: {filepath}")
            continue

        errors = validate_blog(str(filepath))

        if errors:
            files_with_errors += 1
            total_errors += len(errors)
            print(f"\n❌ {filepath}")
            for error in errors:
                print(f"   • {error}")

    print(f"\n{'='*50}")
    if total_errors == 0:
        print(f"✅ All {len(files)} blog(s) passed validation")
        sys.exit(0)
    else:
        print(f"❌ Found {total_errors} error(s) in {files_with_errors} file(s)")
        print("\nSee README.md 'Blogs' section for blog formatting guidelines.")
        sys.exit(1)


if __name__ == '__main__':
    main()
