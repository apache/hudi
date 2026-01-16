#!/usr/bin/env python3
"""
Validate blog post frontmatter for Apache Hudi website.

This script checks newly added or modified blog posts for:
1. Required fields (title, author/authors, category, image, tags)
2. Valid category values
3. Tag format rules
4. Source attribution for external blogs
"""

import os
import re
import sys
from pathlib import Path

# Allowed categories
ALLOWED_CATEGORIES = {'community', 'deep-dive', 'how-to', 'case-study'}

# Tags that should not be used
FORBIDDEN_TAGS = {
    'apache hudi': 'Redundant - every blog on this site is about Apache Hudi',
    'hudi': 'Redundant - every blog on this site is about Apache Hudi',
    'index': 'Use "indexing" instead',
}

# Required frontmatter fields
REQUIRED_FIELDS = ['title', 'category', 'image', 'tags']


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


def check_has_redirect(content: str) -> bool:
    """Check if the blog post redirects to an external URL."""
    # Look for Redirect component or meta refresh
    return 'Redirect' in content or 'http-equiv="refresh"' in content


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

    filename = os.path.basename(filepath)
    is_external = filepath.endswith('.mdx') and check_has_redirect(content)

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in frontmatter:
            # author or authors is acceptable
            if field == 'author' and 'authors' not in frontmatter:
                errors.append(f"Missing required field: 'author' or 'authors'")
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

    # Validate tags
    tags = frontmatter.get('tags', [])
    if isinstance(tags, str):
        tags = [tags]

    if not tags:
        errors.append("At least one tag is required")
    else:
        has_source_tag = False
        for tag in tags:
            tag_lower = tag.lower().strip()

            # Check forbidden tags
            if tag_lower in FORBIDDEN_TAGS:
                errors.append(
                    f"Forbidden tag '{tag}': {FORBIDDEN_TAGS[tag_lower]}"
                )

            # Check for hyphens in tags
            if '-' in tag and not tag.startswith('source:'):
                errors.append(
                    f"Tag '{tag}' contains hyphen. "
                    f"Use spaces instead (e.g., 'apache spark' not 'apache-spark')"
                )

            # Track source attribution
            if tag.startswith('source:'):
                has_source_tag = True

        # External blogs should have source attribution
        if is_external and not has_source_tag:
            errors.append(
                "External blog (with redirect) should have a 'source:' tag "
                "(e.g., 'source:medium', 'source:linkedin')"
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
        print(f"\nSee README.md 'Blogs' section for blog formatting guidelines.")
        sys.exit(1)


if __name__ == '__main__':
    main()
