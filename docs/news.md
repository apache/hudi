---
title: News
sidebar: home_sidebar
keywords: apache, hudi, news, blog, updates, release notes, announcements
permalink: news.html
toc: false
folder: news
---
<div class="home">

    <div class="post-list">   
        {% if page.is_default_language %}
            {% assign firstpPartialPath = 'events' %}
        {% else %}
            {% assign firstpPartialPath = 'cn' %} 
        {% endif %}
          
        {% for post in site.pages limit:10 %}
            {% assign segments = post.url | split: '/' %}
            {% if post.tags contains 'news' and segments[1] == firstpPartialPath %}
                        
    <h2><a class="post-link" href="{{ post.url }}">{{ post.title }}</a></h2>
        <span class="post-meta">{{ post.date | date: "%b %-d, %Y" }} /
            {% for tag in post.tags %}

                <a href="{{ "tag_" | append: tag | append: ".html"}}">{{tag}}</a>{% unless forloop.last %}, {% endunless%}

                {% endfor %}</span>
        <p>{% if page.summary %} {{ page.summary | strip_html | strip_newlines | truncate: 160 }} {% else %} {{ post.content | truncatewords: 50 | strip_html }} {% endif %}</p>

            {% endif %}
        {% endfor %}

        <p><a href="feed.xml" class="btn btn-primary navbar-btn cursorNorm" role="button">RSS Subscribe{{tag}}</a></p>

<hr />
        <p>See more posts from the <a href="news_archive.html">News Archive</a>. </p>


</div>
