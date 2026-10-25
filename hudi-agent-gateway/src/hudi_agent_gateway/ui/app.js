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
 * Chat client for the hudi-agent-gateway. Talks to POST /v1/chat with
 * stream=true and renders the SSE event stream: token / tool_call /
 * tool_result / done / error. First-party code only.
 */
(function () {
  "use strict";

  var messagesEl = document.getElementById("messages");
  var welcomeEl = document.getElementById("welcome");
  var inputEl = document.getElementById("input");
  var sendEl = document.getElementById("send");
  var composerEl = document.getElementById("composer");
  var modelInfoEl = document.getElementById("model-info");

  var modelSelectEl = document.getElementById("model-select");

  var SESSION_KEY = "hudi-agent-gateway.session";
  var MODEL_KEY = "hudi-agent-gateway.model";
  var sessionId = localStorage.getItem(SESSION_KEY) || newSession();
  var selectedModel = null;
  var busy = false;

  function newSession() {
    var id = (crypto.randomUUID && crypto.randomUUID()) ||
      "s-" + Date.now() + "-" + Math.floor(Math.random() * 1e9);
    localStorage.setItem(SESSION_KEY, id);
    return id;
  }

  fetch("../v1/info").then(function (r) { return r.json(); }).then(function (info) {
    modelInfoEl.textContent = info.provider + " · catalog " + info.catalog;
    var base = window.location.origin;
    document.getElementById("ep-trino").textContent = info.trino_url;
    var trinoUi = document.getElementById("ep-trino-ui");
    trinoUi.textContent = info.trino_ui_url;
    trinoUi.href = info.trino_ui_url;
    document.getElementById("ep-chat").textContent = base + "/v1/chat";
    document.getElementById("ep-mcp").textContent = base + "/mcp/";
    document.getElementById("ep-mcp-hint").textContent =
      "for agents: claude mcp add --transport http hudi-lakehouse " + base + "/mcp/";
    if (!info.mcp_enabled) document.getElementById("ep-mcp-row").hidden = true;
  }).catch(function () { modelInfoEl.textContent = "gateway unreachable"; });

  var connectToggle = document.getElementById("connect-toggle");
  var connectPanel = document.getElementById("connect-panel");
  connectToggle.addEventListener("click", function () {
    connectPanel.hidden = !connectPanel.hidden;
    connectToggle.classList.toggle("open", !connectPanel.hidden);
  });
  document.querySelectorAll(".copy").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var text = document.getElementById(btn.dataset.copy).textContent;
      navigator.clipboard.writeText(text).then(function () {
        btn.textContent = "✓";
        setTimeout(function () { btn.textContent = "⎘"; }, 1200);
      });
    });
  });

  // Populate the model picker with whatever the configured provider offers.
  fetch("../v1/models").then(function (r) { return r.json(); }).then(function (m) {
    var remembered = localStorage.getItem(MODEL_KEY);
    selectedModel = (remembered && m.models.indexOf(remembered) !== -1)
      ? remembered : m.default;
    m.models.forEach(function (name) {
      var opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      if (name === selectedModel) opt.selected = true;
      modelSelectEl.appendChild(opt);
    });
    modelSelectEl.hidden = false;
    if (m.models.length <= 1) modelSelectEl.disabled = true;  // e.g. vLLM: one model
    modelSelectEl.addEventListener("change", function () {
      selectedModel = modelSelectEl.value;
      localStorage.setItem(MODEL_KEY, selectedModel);
    });
  }).catch(function () { /* picker simply stays hidden */ });

  document.getElementById("clear-chat").addEventListener("click", function () {
    sessionId = newSession();
    messagesEl.querySelectorAll(".msg, .error-banner").forEach(function (el) { el.remove(); });
    if (welcomeEl) welcomeEl.style.display = "";
    inputEl.focus();
  });

  messagesEl.addEventListener("click", function (e) {
    if (e.target.classList.contains("suggestion")) {
      inputEl.value = e.target.textContent;
      composerEl.requestSubmit();
    }
  });

  inputEl.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      composerEl.requestSubmit();
    }
  });
  inputEl.addEventListener("input", function () {
    inputEl.style.height = "auto";
    inputEl.style.height = Math.min(inputEl.scrollHeight, 160) + "px";
  });

  composerEl.addEventListener("submit", function (e) {
    e.preventDefault();
    var text = inputEl.value.trim();
    if (!text || busy) return;
    inputEl.value = "";
    inputEl.style.height = "auto";
    send(text);
  });

  function el(tag, className, parent) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (parent) parent.appendChild(node);
    return node;
  }

  function scrollToBottom() {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  function addUserMessage(text) {
    if (welcomeEl) welcomeEl.style.display = "none";
    var msg = el("div", "msg user", messagesEl);
    var bubble = el("div", "bubble", msg);
    bubble.textContent = text;
    scrollToBottom();
  }

  function addErrorBanner(text) {
    var banner = el("div", "error-banner", messagesEl);
    banner.textContent = text;
    scrollToBottom();
  }

  function setBusy(value) {
    busy = value;
    sendEl.disabled = value;
  }

  function send(text) {
    addUserMessage(text);
    setBusy(true);

    var msg = el("div", "msg assistant", messagesEl);
    var bubble = el("div", "bubble md", msg);
    var answer = "";
    var cursor = el("span", "cursor", bubble);
    var chips = {};

    function renderAnswer() {
      bubble.innerHTML = window.HudiMarkdown.render(answer);
      if (busy) bubble.appendChild(cursor);
      scrollToBottom();
    }

    var payload = { message: text, session_id: sessionId, stream: true };
    if (selectedModel) payload.model = selectedModel;
    fetch("../v1/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Accept": "text/event-stream" },
      body: JSON.stringify(payload)
    }).then(function (resp) {
      if (!resp.ok || !resp.body) {
        return resp.text().then(function (body) {
          throw new Error("gateway returned " + resp.status + ": " + body.slice(0, 300));
        });
      }
      var reader = resp.body.getReader();
      var decoder = new TextDecoder();
      var buffer = "";

      function pump() {
        return reader.read().then(function (step) {
          if (step.done) { finish(); return null; }
          // Normalize SSE line endings (servers may emit \r\n) before framing.
          buffer += decoder.decode(step.value, { stream: true }).replace(/\r/g, "");
          var frames = buffer.split("\n\n");
          buffer = frames.pop();
          frames.forEach(handleFrame);
          return pump();
        });
      }
      return pump();
    }).catch(function (err) {
      addErrorBanner(err.message || String(err));
      finish();
    });

    function handleFrame(frame) {
      var event = "message";
      var dataLines = [];
      frame.split("\n").forEach(function (line) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
      });
      if (!dataLines.length) return;
      var data;
      try { data = JSON.parse(dataLines.join("\n")); } catch (e) { return; }

      if (event === "token") {
        answer += data.text;
        renderAnswer();
      } else if (event === "tool_call") {
        var chip = el("details", "tool-chip running", bubble.parentNode);
        bubble.parentNode.insertBefore(chip, bubble);
        var summary = el("summary", "", chip);
        summary.textContent = data.name;
        var pre = el("pre", "", chip);
        pre.textContent = JSON.stringify(data.args, null, 2);
        chips[data.id || data.name] = chip;
        scrollToBottom();
      } else if (event === "tool_result") {
        var target = chips[data.id || data.name];
        if (target) {
          target.classList.remove("running");
          if (data.is_error) target.classList.add("error");
          var summaryEl = target.querySelector("summary");
          summaryEl.textContent = data.name + (data.is_error ? " — error" : " ✓");
          var result = el("pre", "", target);
          result.textContent = data.preview;
        }
        scrollToBottom();
      } else if (event === "error") {
        addErrorBanner(data.code + ": " + data.message);
      } else if (event === "done") {
        if (!answer && data.message) {
          answer = data.message;
          renderAnswer();
        }
      }
    }

    function finish() {
      setBusy(false);
      if (cursor.parentNode) cursor.remove();
      if (!answer && !msg.querySelector(".tool-chip")) {
        bubble.innerHTML = "<p><em>(no response)</em></p>";
      } else {
        bubble.innerHTML = window.HudiMarkdown.render(answer);
      }
      scrollToBottom();
      inputEl.focus();
    }
  }
})();
