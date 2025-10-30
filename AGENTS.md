# How to Add a New Blog Post

This repo hosts the Docusaurus website for Hudi. To add a new blog post:

## 1. Blog Markdown File

- Place in: `website/blog/`
- Name: `YYYY-MM-DD-<slug>.md` (date + slug from title)
  - Slug: lowercase, hyphens for spaces/underscores, only letters/numbers/hyphens
  - Example: `2025-11-03-introducing-streaming-compaction-v2.md`

## 2. Images Folder

- Place images in: `website/static/assets/images/blog/<filename-without-md>/`
- Reference images in markdown as:
  - `![Alt text](/assets/images/blog/2025-11-03-introducing-streaming-compaction-v2/diagram.png)`

## 3. Front Matter Example

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

## 4. Content Tips

- Keep paragraphs short.
- Use descriptive alt text for images.
- Add a `description` for SEO.

## 5. Example Directory Layout

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

## 6. Commit

- Only change files under `website/`.
- Commit message: `docs(blog): add YYYY-MM-DD-<slug>`

Your post is now ready for review.
