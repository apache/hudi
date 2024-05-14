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

package org.apache.hudi.utilities;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.ValidationUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.javalin.Javalin;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class HoodieWithTimelineServer implements Serializable {

  private static final long serialVersionUID = 1L;
  private final Config cfg;

  public HoodieWithTimelineServer(Config cfg) {
    this.cfg = cfg;
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master")
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = true)
    public String sparkMemory = null;
    @Parameter(names = {"--num-partitions", "-n"}, description = "Num Partitions")
    public Integer numPartitions = 100;
    @Parameter(names = {"--server-port", "-p"}, description = " Server Port")
    public Integer serverPort = 26754;
    @Parameter(names = {"--delay-secs", "-d"}, description = "Delay(sec) before client connects")
    public Integer delaySecs = 30;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public void startService() {
    Javalin app = Javalin.create().start(cfg.serverPort);
    app.get("/", ctx -> ctx.result("Hello World"));
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    HoodieWithTimelineServer service = new HoodieWithTimelineServer(cfg);
    service.run(UtilHelpers.buildSparkContext("client-server-hoodie", cfg.sparkMaster, cfg.sparkMemory));
  }

  public void run(JavaSparkContext jsc) throws UnknownHostException {
    startService();
    final String driverHost = InetAddress.getLocalHost().getHostAddress();
    System.out.println("Driver Hostname is :" + driverHost);
    List<String> messages = new ArrayList<>();
    IntStream.range(0, cfg.numPartitions).forEach(i -> messages.add("Hello World"));

    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    context.setJobStatus(this.getClass().getSimpleName(), "Sending requests to driver host");
    List<String> gotMessages = context.map(messages, msg -> sendRequest(driverHost, cfg.serverPort), messages.size());
    System.out.println("Got Messages :" + gotMessages);
    ValidationUtils.checkArgument(gotMessages.equals(messages), "Got expected reply from Server");
  }

  public String sendRequest(String driverHost, int port) {
    String url = String.format("http://%s:%d/", driverHost, port);
    try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

      System.out.println("Sleeping for " + cfg.delaySecs + " secs ");
      Thread.sleep(cfg.delaySecs * 1000L);
      System.out.println("Woke up after sleeping for " + cfg.delaySecs + " secs ");

      HttpGet request = new HttpGet(url);

      HttpResponse response = client.execute(request);

      System.out.println("Response Code from(" + url + ") : " + response.getStatusLine().getStatusCode());

      try (BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
        StringBuilder result = new StringBuilder();
        rd.lines().forEach(result::append);
        System.out.println("Got result (" + result + ")");
        return result.toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
