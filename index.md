---
layout: page
title: Welcome!
tagline: Supporting tagline
---
{% include JB/setup %}

Wow, real fun!

This is my English blog(<a href="http://2maomao.com/blog/">Chinese blog</a>).

Recent Posts:

<ul class="posts">
  {% for post in site.posts %}
    <li><span>{{ post.date | date_to_string }}</span> &raquo; <a href="{{ BASE_PATH }}{{ post.url }}">{{ post.title }}</a></li>
  {% endfor %}
</ul>

