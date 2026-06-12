/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.timeline.service.ui;

import io.javalin.Javalin;
import io.javalin.http.ContentType;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Main User Interface (UI) Handler class that is responsible for registering UI routes.
 * Static files (HTML, JS, CSS) are served by Javalin's built-in static file serving
 * from the classpath at /public/.
 */
public class UiHandler {

  private final Javalin app;

  public UiHandler(Javalin app) {
    this.app = app;
  }

  public void register() {
    // Read /public/index.html from the classpath as an InputStream and write its contents
    // directly into the response body with ctx.result(...). From the browser's perspective,
    // the server responded to /ui with HTML content — no redirect happened, so the URL
    // stays as /ui.
    app.get("/ui", ctx -> {
      InputStream is = getClass().getResourceAsStream("/public/index.html");
      if (is == null) {
        ctx.status(404).result("UI page not found");
        return;
      }
      try (is) {
        ctx.contentType(ContentType.HTML);
        ctx.result(new String(is.readAllBytes(), StandardCharsets.UTF_8));
      }
    });
  }
}
