## Site Documentation

This folder contains resources that build the [Apache Hudi website](https://hudi.apache.org)


### Building docs

The site is based on a [Jekyll](https://jekyllrb.com/) theme hosted [here](https://github.com/mmistakes/minimal-mistakes/) with detailed instructions.

#### Docker

Simply run `docker-compose build --no-cache && docker-compose up` from the `docs` folder and the site should be up & running at `http://localhost:4000`

To see edits reflect on the site, you may have to bounce the container

 - Stop existing container by `ctrl+c` the docker-compose program
 - (or) alternatively via `docker stop docs_server_1`
 - Bring up container again using `docker-compose up`

#### Host OS

To build directly on host OS (\*nix), first you need to install

- gem, ruby (using apt-get/brew)
- bundler (`gem install bundler`)
- jekyll (`gem install jekyll`)
- Update bundler `bundle update --bundler`

and then run the following commands from `docs` folder to install dependencies

`bundle install`

and serve a local site

`bundle exec jekyll serve`

### Submitting changes

To submit changes to the docs, please make the changes on the `asf-site` branch, build the site locally, test it out and submit a pull request with the changes to .md and theme files under `docs`

### Updating site

Once a pull request merged, Travis CI will regenerate the site and move the generated site from `_site` to `docs/../content` and then submit changes as a PR automatically.

### Adding docs for version

During each release, we must preserve the old version's docs so users on that version can refer to it. 
Below documents the steps needed to do that. 

#### Make a copy of current docs 

Copy the docs as-is into another folder

```
cd docs/_docs
export VERSION=0.5.0
mkdir -p $VERSION && cp *.md $VERSION/
```

#### Rewrite links & add version to each page

This step changes the permalink (location where these pages would be placed) with a version prefix and also changes links to each other.

Mac users please use these commands:
```
cd $VERSION
sed -i '' -e "s/permalink: \/docs\//permalink: \/docs\/${VERSION}-/g" *.md
sed -i '' -e "s/permalink: \/cn\/docs\//permalink: \/cn\/docs\/${VERSION}-/g" *.cn.md
sed -i '' -e "s/](\/docs\//](\/docs\/${VERSION}-/g" *.md
sed -i '' -e "s/](\/cn\/docs\//](\/cn\/docs\/${VERSION}-/g" *.cn.md
for f in *.md; do [ -f $f ] &&  sed -i '' -e "1s/^//p; 1s/^.*/version: ${VERSION}/" $f; done
```

Non Mac please use these:
```
cd $VERSION
sed -i "s/permalink: \/docs\//permalink: \/docs\/${VERSION}-/g" *.md
sed -i "s/permalink: \/cn\/docs\//permalink: \/cn\/docs\/${VERSION}-/g" *.cn.md
sed -i "s/](\/docs\//](\/docs\/${VERSION}-/g" *.md
sed -i "s/](\/cn\/docs\//](\/cn\/docs\/${VERSION}-/g" *.cn.md
sed -i "0,/---/s//---\nversion: ${VERSION}/" *.md
```

#### Reworking site navigation

In `_config.yml`, add a new author section similar to `0.5.0_author`. Then, change `quick_link.html` with a if block to use this navigation, when the new version's page is rendered
  
```
{%- if page.language == "0.5.0" -%}
  {%- assign author = site.0.5.0_author -%}
{%- else -%}
  {%- assign author = site.author -%}
{%- endif -%}
```

Then in `navigation.yml`, add a new section similar to `0.5.0_docs` (or the last release), with each link pointing to pages starting with `$VERSION-`. Change `nav_list` with else-if to 
render the new version's equivalent navigation links. 

```
{% if page.version %}
    {% if page.version == "0.5.0" %}
        {% assign navigation = site.data.navigation["0.5.0_docs"] %}
    {% endif %}
{% endif %}
```

Final steps:
 - In `_config.yml` add a new subsection under `previous_docs: ` for this version similar to `  - version: 0.5.0`
 - Edit `docs/_pages.index.md` to point to the latest release. Change the text of latest release and edit the href 
 link to point to the release tag in github.
 - in `docs/_pages/releases.md` Add a new section on the very top for this release. Refer to `Release 0.5.0-incubating` 
 for reference. Ensure the links for github release tag, docs, source release, raw release notes are pointing to this 
 latest release. Also include following subsections - `Download Information`, `Release Highlights` and `Raw Release Notes`.
 
#### Link to this version's doc





