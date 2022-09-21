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

- Remember to include the `.md` extension.
- Files will be linked to correct corresponding version.
- Relative paths work as well.

```md
The [@hello](hello.md#paginate) document is great!

See the [Tutorial](../getting-started/tutorial.md) for more info.
```

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
3. The image must be uploaded in the path /assets/images/blog/<image_file-name> and should be of standard size 1200 * 600
4. The tags should be representative of these
   1. tag1
      - how-to (tutorial, recipes, show case how to use feature x)
      - use-case (some community users talking about their use-case)
      - design (technical articles talking about Hudi internal design/impl)
      - performance (involves performance related blogs)
   2. tag 2
       - Represent individual features - clustering, compaction, ingestion, meta-sync etc.
   3. tag 3
      - Source. This is usually the second level domain name for this article gathered from the url link.
       For example if the article is https://www.uber.com/blog/cost-efficiency-big-data/ we would use `uber` as the tag here. 
       Another example - for https://robinhood.engineering/author-balaji-varadarajan-e3f496815ebf  we would use 
       `robinhood` as the tag. For blogs directly contributed to hudi repo, we can use `apache hudi` as the tag.
      
## Maintainer

Apache Hudi Community
