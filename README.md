# Apache Hudi Website Source Code

This repo hosts the source code of [Apache Hudi Official Website](https://hudi.apache.org/).

# Prerequisite

Install [npm](https://treehouse.github.io/installation-guides/mac/node-mac.html) for the first time.

# Test Website

Build from source
```bash
./website/scripts/build-site.sh
```

The results are moved to directory: `content`

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

## Testing your Build Locally {#testing-build-locally}

It is important to test your build locally before deploying to production.

```console
cd website
npm run serve
```

## To Add New Docs Version

To better understand how versioning works and see if it suits your needs, you can read on below.

## Directory structure {#directory-structure}

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

### Tagging a new version {#tagging-a-new-version}

1. First, make sure your content in the `docs` directory is ready to be frozen as a version. A version always should be based from master.
1. Enter a new version number.

```bash npm
npm run docusaurus docs:version 0.8.0
```

When tagging a new version, the document versioning mechanism will:

- Copy the full `docs/` folder contents into a new `versioned_docs/version-<version>/` folder.
- Create a versioned sidebars file based from your current [sidebar](docs-introduction.md#sidebar) configuration (if it exists) - saved as `versioned_sidebars/version-<version>-sidebars.json`.
- Append the new version number to `versions.json`.

## Docs {#docs}

### Creating new docs {#creating-new-docs}

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

### Linking docs {#linking-docs}

- Remember to include the `.md` extension.
- Files will be linked to correct corresponding version.
- Relative paths work as well.

```md
The [@hello](hello.md#paginate) document is great!

See the [Tutorial](../getting-started/tutorial.md) for more info.
```

## Versions {#versions}

Each directory in `versioned_docs/` will represent a documentation version.

### Updating an existing version {#updating-an-existing-version}

You can update multiple docs versions at the same time because each directory in `versioned_docs/` represents specific routes when published.

1. Edit any file.
1. Commit and push changes.
1. It will be published to the version.

Example: When you change any file in `versioned_docs/version-0.7.0/`, it will only affect the docs for version `0.7.0`.

## Maintainer

Apache Hudi Community
