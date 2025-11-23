# Agents Instructions

This repo hosts the Docusaurus website for Hudi.

## Adding a New Blog Post

To add a new blog post:

### Blog Markdown File

- Place in: `website/blog/`
- Name: `YYYY-MM-DD-<slug>.md` (date + slug from title)
  - Slug: lowercase, hyphens for spaces/underscores, only letters/numbers/hyphens
  - Example: `2025-11-03-introducing-streaming-compaction-v2.md`

### Images Folder

- Place images in: `website/static/assets/images/blog/<filename-without-md>/`
- Reference images in markdown as:
  - `![Alt text](/assets/images/blog/2025-11-03-introducing-streaming-compaction-v2/diagram.png)`

### Front Matter Example

```yaml
---
title: "Your Blog Title"
excerpt: ""
author: ""
category: blog
image: /assets/images/blog/<your-folder>/og.png
tags:
  - hudi
---
```

### Content Tips

- Keep paragraphs short.
- Use descriptive alt text for images.
- Add a `description` for SEO.

### Example Directory Layout

```text
website/
├─ blog/
│  └─ 2025-11-03-introducing-streaming-compaction-v2.md
└─ static/
   └─ assets/
      └─ images/
         └─ blog/
            └─ 2025-11-03-introducing-streaming-compaction-v2/
               ├─ og.png
               └─ diagram.png
```

### Commit

- Only change files under `website/`.
- Commit message: `docs(blog): add YYYY-MM-DD-<slug>`

Your post is now ready for review.

## Updating Documentation Pages

The `website/docs` directory contains documentation pages for the Hudi project, which are versioned against releases.

When updating a markdown file in the hudi website repo, if the file contains a `last_modified_at` field in its front matter, update it to the current date when modifying the file.
