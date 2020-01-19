---
version: 0.5.0
title: Docs Versions
keywords: hudi, privacy
permalink: /docs/0.5.0-docs-versions.html
last_modified_at: 2019-12-30T15:59:57-04:00
---

<table>
    <tbody>
      {% for d in site.previous_docs %}
        <tr>
            <th>{{ d.version }}</th>
            <td><a href="{{ d.en }}">English Version</a></td>
            <td><a href="{{ d.cn }}">Chinese Version</a></td>
        </tr>
      {% endfor %}
    </tbody>
</table>
