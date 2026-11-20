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

/**
 * TEST ONLY. ImageIO spy providers for the handle release tests of the embedding image code.
 *
 * <p>The package name is deliberate. The product code picks a JDK built-in ImageIO reader or writer by the class name of its
 * provider: {@code ImageHeaderReader.isJdkProvider} accepts a provider whose class name
 * starts with {@code com.sun.imageio.plugins.} and disposes every other one. A spy provider must lie under that prefix to become
 * the chosen reader or writer, the one whose release the tests check; a real JDK reader or writer does not report whether it
 * was disposed. {@code com.sun.imageio.plugins.leakspy} is not a package of the {@code java.desktop} module, so it is no split
 * package on JDK 9+.
 *
 * <p>Nothing registers these providers through {@code META-INF/services}: a test registers them from code for its own duration
 * and deregisters them after ({@code ImageProviderLeakSpies}). They recognise only their own test content and format, except the
 * spy JPEG writer {@code ImageProviderLeakSpies.installJpeg} puts ahead of the JDK's for its own duration. The classes ship only in the test jar.
 */
package com.sun.imageio.plugins.leakspy;
