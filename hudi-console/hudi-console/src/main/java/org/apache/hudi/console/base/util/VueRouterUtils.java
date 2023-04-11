/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.console.base.util;

import org.apache.hudi.console.base.domain.router.RouterTree;
import org.apache.hudi.console.base.domain.router.VueRouter;

import java.util.ArrayList;
import java.util.List;

public final class VueRouterUtils {

  private VueRouterUtils() {}

  private static final String TOP_NODE_ID = "0";

  /**
   * build menu or department tree
   *
   * @param nodes nodes
   * @param <T> <T>
   * @return <T> Tree<T>
   */
  public static <T> RouterTree<T> buildRouterTree(List<RouterTree<T>> nodes) {
    if (nodes == null) {
      return null;
    }
    List<RouterTree<T>> topNodes = new ArrayList<>();
    nodes.forEach(
        node -> {
          String pid = node.getParentId();
          if (pid == null || TOP_NODE_ID.equals(pid)) {
            topNodes.add(node);
            return;
          }
          for (RouterTree<T> n : nodes) {
            String id = n.getId();
            if (id != null && id.equals(pid)) {
              if (n.getChildren() == null) {
                n.initChildren();
              }
              n.getChildren().add(node);
              node.setHasParent(true);
              n.setHasChildren(true);
              n.setHasParent(true);
              return;
            }
          }
          if (topNodes.isEmpty()) {
            topNodes.add(node);
          }
        });

    RouterTree<T> root = new RouterTree<>();
    root.setId("0");
    root.setParentId("");
    root.setHasParent(false);
    root.setHasChildren(true);
    root.setChildren(topNodes);
    root.setText("root");
    return root;
  }

  /**
   * build vue router
   *
   * @param routes routes
   * @param <T> T
   * @return ArrayList<VueRouter < T>>
   */
  public static <T> ArrayList<VueRouter<T>> buildVueRouter(List<VueRouter<T>> routes) {
    if (routes == null) {
      return null;
    }
    List<VueRouter<T>> topRoutes = new ArrayList<>();
    routes.forEach(
        route -> {
          String parentId = route.getParentId();
          if (parentId == null || TOP_NODE_ID.equals(parentId)) {
            topRoutes.add(route);
            return;
          }
          for (VueRouter<T> parent : routes) {
            String id = parent.getId();
            if (parentId.equals(id)) {
              if (parent.getChildren() == null) {
                parent.initChildren();
              }
              parent.getChildren().add(route);
              parent.setHasChildren(true);
              route.setHasParent(true);
              parent.setHasParent(true);
              return;
            }
          }
        });

    ArrayList<VueRouter<T>> list = new ArrayList<>();
    VueRouter<T> root = new VueRouter<>();
    root.setName("Root");
    root.setComponent("BasicView");
    root.setPath("/");
    root.setChildren(topRoutes);
    list.add(root);

    return list;
  }
}
