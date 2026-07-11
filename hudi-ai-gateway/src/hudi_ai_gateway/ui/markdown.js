/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Minimal first-party markdown renderer for the subset the gateway's agent
 * emits: paragraphs, bold/italic, inline + fenced code, links, lists, and
 * tables. All input is HTML-escaped before any markup is applied, so the
 * output is XSS-safe by construction. No third-party code.
 */
(function () {
  "use strict";

  function escapeHtml(s) {
    return s
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function inline(s) {
    return s
      .replace(/`([^`]+)`/g, function (_, code) { return "<code>" + code + "</code>"; })
      .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
      .replace(/(^|[^*])\*([^*\s][^*]*)\*/g, "$1<em>$2</em>")
      .replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g,
        '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
  }

  function isTableDivider(line) {
    return /^\s*\|?[\s:|-]+\|[\s:|-]*$/.test(line) && line.indexOf("-") !== -1;
  }

  function splitRow(line) {
    var t = line.trim();
    if (t.startsWith("|")) t = t.slice(1);
    if (t.endsWith("|")) t = t.slice(0, -1);
    return t.split("|").map(function (c) { return c.trim(); });
  }

  function render(md) {
    var lines = escapeHtml(md).split("\n");
    var html = [];
    var i = 0;

    while (i < lines.length) {
      var line = lines[i];

      if (/^\s*```/.test(line)) {                       // fenced code block
        var code = [];
        i++;
        while (i < lines.length && !/^\s*```/.test(lines[i])) { code.push(lines[i]); i++; }
        i++; // closing fence
        html.push("<pre><code>" + code.join("\n") + "</code></pre>");
        continue;
      }

      if (line.indexOf("|") !== -1 && i + 1 < lines.length && isTableDivider(lines[i + 1])) {
        var head = splitRow(line);
        i += 2;
        var body = [];
        while (i < lines.length && lines[i].indexOf("|") !== -1 && lines[i].trim() !== "") {
          body.push(splitRow(lines[i])); i++;
        }
        var t = "<table><thead><tr>";
        head.forEach(function (h) { t += "<th>" + inline(h) + "</th>"; });
        t += "</tr></thead><tbody>";
        body.forEach(function (row) {
          t += "<tr>";
          row.forEach(function (c) { t += "<td>" + inline(c) + "</td>"; });
          t += "</tr>";
        });
        t += "</tbody></table>";
        html.push(t);
        continue;
      }

      if (/^\s*(?:[-*]|\d+\.)\s+/.test(line)) {          // list block
        var ordered = /^\s*\d+\./.test(line);
        var items = [];
        while (i < lines.length && /^\s*(?:[-*]|\d+\.)\s+/.test(lines[i])) {
          items.push(lines[i].replace(/^\s*(?:[-*]|\d+\.)\s+/, "")); i++;
        }
        var tag = ordered ? "ol" : "ul";
        html.push("<" + tag + ">" + items.map(function (it) {
          return "<li>" + inline(it) + "</li>";
        }).join("") + "</" + tag + ">");
        continue;
      }

      var h = line.match(/^(#{1,4})\s+(.*)$/);           // headings
      if (h) {
        var level = Math.min(h[1].length + 2, 6);        // demote: chat bubbles, not documents
        html.push("<h" + level + ">" + inline(h[2]) + "</h" + level + ">");
        i++;
        continue;
      }

      if (line.trim() === "") { i++; continue; }

      var para = [line];                                  // paragraph (merge soft-wrapped lines)
      i++;
      while (i < lines.length && lines[i].trim() !== "" &&
             !/^\s*(?:```|[-*]\s|\d+\.\s|#)/.test(lines[i]) &&
             !(lines[i].indexOf("|") !== -1 && i + 1 < lines.length && isTableDivider(lines[i + 1]))) {
        para.push(lines[i]); i++;
      }
      html.push("<p>" + inline(para.join(" ")) + "</p>");
    }

    return html.join("\n");
  }

  window.HudiMarkdown = { render: render, escapeHtml: escapeHtml };
})();
