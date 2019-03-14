## Site Documentation

This folder contains resources that build the [Apache Hudi website](https://hudi.apache.org)


### Building docs

The site is based on a [Jekyll](https://jekyllrb.com/) theme hosted [here](idratherbewriting.com/documentation-theme-jekyll/) with detailed instructions.

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

and then run the following from `docs` folder to serve a local site

`bundle exec jekyll serve`

### Submitting changes

To submit changes to the docs, please make the changes on the `asf-site` branch, build the site locally, test it out and submit a pull request with the changes to .md and theme files under `docs`

### Updating site

At a regular cadence, one of the Hudi committers will regenerate the site. In order to do this, first build it locally, test and then move the generated site from `_site` locally to `docs/../content`. Submit changes as a PR.

### Automation
Coming soon.
