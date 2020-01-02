FROM jekyll/jekyll:3.8

WORKDIR /tmp
ADD Gemfile /tmp/
ADD Gemfile.lock /tmp/
RUN chmod a+w Gemfile.lock


## pick up speed in china
#RUN gem sources -l \
# && gem sources --add https://gems.ruby-china.com/ --remove https://rubygems.org/ \
# && gem sources -u \
# && bundle config mirror.https://rubygems.org https://gems.ruby-china.com \
# && bundle config --delete 'mirror.https://rubygems.org/' \
# && sed -i "s/rubygems.org/gems.ruby-china.com/" Gemfile \
# && sed -i "s/rubygems.org/gems.ruby-china.com/" Gemfile.lock


RUN bundle install \
 && bundle update --bundler

VOLUME /src
EXPOSE 4000

WORKDIR /src
ENTRYPOINT ["bundle", "exec", "jekyll", "serve", "--force_polling", "-H", "0.0.0.0", "-P", "4000"]

