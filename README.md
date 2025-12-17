# Apache Hudi Website Source Code

This repo hosts the source code of [Apache Hudi Official Website](https://hudi.apache.org/).

## Prerequisite

Install [npm](https://treehouse.github.io/installation-guides/mac/node-mac.html) for the first time.

## Installation

```console
cd website
npm install
```

## Local Development

```console
cd website
npm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

## Build

```console
cd website
npm run build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Testing your Build Locally

It is important to test your build locally before deploying to production.

```console
cd website
npm run serve
```

## Build script

Build from source

```bash
./website/scripts/build-site.sh
```

The results are moved to directory: `content`

## To Add New Docs Version

To better understand how versioning works and see if it suits your needs, you can read on below.

## Directory structure

```shell
website
├── sidebars.js          # sidebar for master (next) version
├── docs                 # docs directory for master (next) version
│   └── hello.md         # https://mysite.com/docs/next/hello
├── versions.json        # file to indicate what versions are available
├── versioned_docs
│   ├── version-0.7.0
│   │   └── hello.md     # https://mysite.com/docs/0.7.0/hello
│   └── version-0.8.0
│       └── hello.md     # https://mysite.com/docs/hello
├── versioned_sidebars
│   ├── version-0.7.0-sidebars.json
│   └── version-0.8.0-sidebars.json
├── docusaurus.config.js
└── package.json
```

The table below explains how a versioned file maps to its version and the generated URL.

| Path                                    | Version        | URL               |
| --------------------------------------- | -------------- | ----------------- |
| `versioned_docs/version-0.7.0/hello.md` | 0.7.0          | /docs/0.7.0/hello |
| `versioned_docs/version-0.8.0/hello.md` | 0.8.0 (latest) | /docs/hello       |
| `docs/hello.md`                         | next           | /docs/next/hello  |

### Tagging a new version

1. First, make sure your content in the `docs` directory is ready to be frozen as a version. A version always should be based from master.
2. Enter a new version number.

```bash npm
npm run docusaurus docs:version 0.8.0
```

When tagging a new version, the document versioning mechanism will:

- Copy the full `docs/` folder contents into a new `versioned_docs/version-<version>/` folder.
- Create a versioned sidebars file based from your current [sidebar](docs-introduction.md#sidebar) configuration (if it exists) - saved as `versioned_sidebars/version-<version>-sidebars.json`.
- Append the new version number to `versions.json`.

3. We have few hard coded versions to updated manually. Do fix them in docusaurus.config.js
   Example commit used when generating docs for 0.10.1 : https://github.com/apache/hudi/pull/4703/commits/b474ec266fe2243f8146ba7a112045bbc8b0ddc8
   This commit has changes for both docs and release highlights. Please update as per necessity.
4. In addition to docusaurus.config.js, we need to manually change the link of `Latest releases` inside `website/src/components/HomepageHeader/index.js` to point to this new release. Previously this was fixed inside `website/src/pages/index.js`. going forward this needs to be fixed inside `website/src/components/HomepageHeader/index.js`.

## Docs

### Creating new docs

1. Place the new file into the corresponding version folder.
1. Include the reference for the new file into the corresponding sidebar file, according to version number.

**Master docs**

```shell
# The new file.
docs/new.md

# Edit the corresponding sidebar file.
sidebar.js
```

**Older docs**

```shell
# The new file.
versioned_docs/version-0.7.0/new.md

# Edit the corresponding sidebar file.
versioned_sidebars/version-0.7.0-sidebars.json
```

### Linking docs

**IMPORTANT: Always use just the filename with `.md` extension when linking to other docs.**

Docusaurus resolves file paths with `.md` extension at build time, converting them to absolute URLs. This ensures:

- Links work correctly regardless of trailing slashes in URLs
- Links automatically resolve to the correct version in versioned docs

**DO NOT use `../` or `./` prefixes** - these break versioned docs behavior.

#### Versioned docs linking

Referring to versioned docs pages within `docs/` MUST USE relative paths with `.md` extension.

**Good Example of linking:**

Say we are updating a 0.12.0 version doc which is older.

```md
A [callback notification](writing_data.md#commit-notifications) is exposed
```

This automatically resolves to /docs/0.12.0/writing_data#commit-notifications.

**Bad examples of linking:**

```md
A [callback notification](/docs/writing_data#commit-notifications) is exposed
```

This will resolve to the most recent release, specifically /docs/writing_data#commit-notifications. We do not want a 0.12.0 doc page to point to a page from a later release.

```md
A [callback notification](writing_data#commit-notifications) is exposed
```

Without `.md` extension, this becomes a relative URL that the browser resolves at runtime, which can break when the page URL has a trailing slash.

```md
A [callback notification](../writing_data.md#commit-notifications) is exposed
```

Using `../` or `./` prefixes breaks versioned docs behavior and should be avoided.

#### DO NOT use next version when linking

**Good Example** - linking when you are working on unreleased version (from next version):

```md
Hudi adopts Multiversion Concurrency Control (MVCC), where [compaction](compaction.md) action merges logs and base files to produce new
file slices and [cleaning](cleaning.md) action gets rid of unused/older file slices to reclaim space on the file system.
```

This automatically resolves to /docs/next/compaction and /docs/next/cleaning pages.

**Bad Example** - linking when you are working on unreleased version (from next version):

```md
Hudi adopts Multiversion Concurrency Control (MVCC), where [compaction](/docs/next/compaction) action merges logs and base files to produce new
file slices and [cleaning](/docs/next/cleaning) action gets rid of unused/older file slices to reclaim space on the file system.
```

Even though it directly points to /docs/next which is intended target, this accumulates as tech debt when this copy of docs gets released, we will have an older doc always pointing to /docs/next/.

#### Linking from outside docs

Refer to `/docs/...` or `/docs/<version>/...` (when a specific version is needed) from pages that are outside of `/docs/` dir.

## Versions

Each directory in `versioned_docs/` will represent a documentation version.

### Updating an existing version

You can update multiple docs versions at the same time because each directory in `versioned_docs/` represents specific routes when published.

1. Edit any file.
2. Commit and push changes.
3. It will be published to the version.

Example: When you change any file in `versioned_docs/version-0.7.0/`, it will only affect the docs for version `0.7.0`.

## Configs

Configs can be automatically updated by following these steps documented at ../hudi-utils/README.md

## Events Banner

### How to disable Events banner in the main page - https://hudi.apache.org/?

Remove `<Events />` row from the file `website/src/pages/index.js`. This should be found below `<HomepageHeader />`. Note commenting out has no effect. This entry has to
be removed to turn off the banner.

### How to add a new Events banner ?

The code for displaying an event banner is already in place. Steps to add:

1. Edit file `website/src/components/EventFeature/index.js` . Edit Event description, date and note if it is an
   `In Person Event` or a `Linkedin Live Event`.
2. Add `<Events />` entry after `<HomepageHeader />` (if not already present) in the file `website/src/pages/index.js`

## Talks

When adding a talk, please follow these guidelines.

1. Ensure the entry is of the format
   "[Title](Hyperlink to video/resources)" - By <Author 1>, <Author 2>, <Author 3>. <Name of Conference/Meetup/Session>, <Month> <Year>.
2. Please ensure the talks are in chronological order.
3. Try to add links to videos and slide decks when possible. If they are not available in same page, feel free to add
   [Slides](Slides link) towards the end like for example:

:::note
["Hoodie: An Open Source Incremental Processing Framework From Uber"](http://www.dataengconf.com/hoodie-an-open-source-incremental-processing-framework-from-uber) - By Vinoth Chandar.
Apr 2017, DataEngConf, San Francisco, CA [Slides](https://www.slideshare.net/vinothchandar/hoodie-dataengconf-2017) [Video](https://www.youtube.com/watch?v=7Wudjc-v7CA)
:::

## Blogs

When adding a new blog, please follow these guidelines.

1. Every Blog should have the `title`, `authors`, `image`, `tags` in the metadata of the blog. For example the front matter
   for a blog should look like below.

```
---
title: "Blog title"
author: FirstName LastName
category: blog
image: /assets/images/blog/<image_file>
tags:
- how-to
- deltastreamer
- incremental-processing
- apache hudi
---
```

2. The blog can be inline or referring to an external blog. If its an inline blog please save it as `.md` file.
   Example for an inline blog - (Build Open Lakehouse using Apache Hudi & dbt)[https://github.com/apache/hudi/blob/asf-site/website/blog/2022-07-11-build-open-lakehouse-using-apache-hudi-and-dbt.md].
   If the blog is referring to an external blog you would need to embed the redirect url and save it as a `.mdx` file.
   Take a look at this blog for reference - (Apache Hudi vs Delta Lake vs Apache Iceberg - Lakehouse Feature Compariso)[https://raw.githubusercontent.com/apache/hudi/asf-site/website/blog/2022-08-18-Apache-Hudi-vs-Delta-Lake-vs-Apache-Iceberg-Lakehouse-Feature-Comparison.mdx]
3. The image must be uploaded in the path /assets/images/blog/<image_file-name> and should be of standard size 1200 \* 600
4. The tags should be representative of these
   1. tag1
      - how-to (tutorial, recipes, show case how to use feature x)
      - use-case (some community users talking about their use-case)
      - design (technical articles talking about Hudi internal design/impl)
      - performance (involves performance related blogs)
      - blog (anything else such as announcements/release updates/insights/guides/tutorials/concepts overview etc)
   2. tag 2
      - Represent individual features - clustering, compaction, ingestion, meta-sync etc. Make sure you keep the features **singular**, i.e., Use `upsert` not `upserts` or use `delete` not `deletes`
   3. tag 3
      - Source. This is usually the second level domain name for this article gathered from the url link.
        For example if the article is https://www.uber.com/blog/cost-efficiency-big-data/ we would use `uber` as the tag here.
        Another example - for https://robinhood.engineering/author-balaji-varadarajan-e3f496815ebf we would use
        `robinhood` as the tag. For blogs directly contributed to hudi repo, we can use `apache hudi` as the tag.
   4. Please refer to guidelines for tagging [below](guidelines-to-tag-properly)

## Video Guides

When adding a new video guide, please follow these guidelines.

1. Every video guide should have the `title`, `last_modified_at`, `authors`, `image`, `navigate`, `tags` in the metadata
   of the video guide. For example the front matter for a video guide should look like below.

```
---
title: "Video guide title"
last_modified_at: <Time in this format - 2023-10-13T16:54:38.964863-07:00>
authors:
- name: FirstName LastName
- name: FirstName LastName
category: blog
image: /assets/images/video_blogs/<image_file>
navigate: "<youtube/any other video link>"
tags:
- guide
- hands-on
- csv
- aws glue
- apache hudi
---
```

2. The video guide should be named as a `<yyyy-mm-dd>-<Video Guide Title>.md` file where date part of the file name
   represents date published.
3. The image must be uploaded in the path /assets/images/video_blogs/<image_file-name> and should be of standard size
   1200 \* 600. Its easy to use the same name as the video guide for the image as well - `<yyyy-mm-dd>-<Video Guide Title>.png`
   If there is no thumbnail or cover image stick to the default image - `/assets/images/hudi-video-page-default.png`
   (OR) another alternative would be to create a simple cover image with the title of the video as the thumbnail.
4. The navigate field represents the actual link to video.
5. The tags should be representative of Hudi component/feature focussed in the guide. The tags can refer to services or
   techniques used in the guide. Stick to 5 - 7 tags at the max.
   1. tag 1
      - code-walkthrough
      - guide (represents hands-on labs, labs)
   2. tag 2
      Can be the use case or functionality achieved. Example - de-duplication. This can be skipped if the guide is in
      general talking about ingestion using different sources and sinks.
   3. tag 3
      - Represent individual Hudi features/components - clustering, compaction, ingestion, meta-sync etc.
      - If deltastreamer is referred, add both tags - `deltastreamer` and `hudi streamer` since we renamed deltastreamer.
   4. tag 4
      - List of technologies used in the guide. This should be an inclusive list. Qualify names fully here. For example
        prefer to "amazon athena" instead of "athena". This helps in discoverability of the guides.
   5. tag N
      - [beginner, intermediate, advanced]. Use this tag if its clear on what level this guide targets. Else feel
        free to skip this tag.
   6. Please refer to guidelines for tagging [below](guidelines-to-tag-properly)
6. Ensure that tags are consistent. When adding new tags refer [Blog Tags](https://hudi.apache.org/blog/tags) and
   [Video Guide Tags](https://hudi.apache.org/videos/tags) to check if there is a tag already and prefer to use that.

## Guidelines to tag properly

      - Please do not use `index` as a tag. Instead, use `indexing`.
      - Please do not use `-` in tags, since it may affect discoverability.
      - When referring to a specific type of query like `snapshot query` with query as the suffix use singular.
      - When referring to write operation types prefer plural. Ex: `inserts` over `insert`.
      - Avoid class names

## Maintainer

Apache Hudi Community
