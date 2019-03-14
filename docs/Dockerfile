FROM ruby:2.6

RUN apt-get clean \
  && mv /var/lib/apt/lists /var/lib/apt/lists.broke \
  && mkdir -p /var/lib/apt/lists/partial

RUN apt-get update

RUN apt-get install -y \
    nodejs \
    python-pygments \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/

WORKDIR /tmp
ADD Gemfile /tmp/
ADD Gemfile.lock /tmp/

RUN gem install bundler
RUN gem install jekyll
RUN bundle install
RUN bundle update --bundler
 

VOLUME /src
EXPOSE 4000

WORKDIR /src
ENTRYPOINT ["bundle", "exec", "jekyll", "serve", "--force_polling", "-H", "0.0.0.0", "-P", "4000"]

