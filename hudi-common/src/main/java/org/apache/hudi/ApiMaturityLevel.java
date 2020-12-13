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

package org.apache.hudi;

/**
 * Indicates how stable a given API method/class is, so user's can plan and set their expectations accordingly.
 */
public enum ApiMaturityLevel {
    /**
     * New APIs start out in this state. Although enough thought will be given to avoid
     * breaking changes to the API in the future, sometimes it might need to change
     * based on feedback.
     */
    EVOLVING,
    /**
     * Enough applications/users have picked up the API and we deem it stable. We will strive to never
     * break the stability of such APIs within a given major version release.
     */
    STABLE,
    /**
     * New things are born, old things fade away. This holds true for APIs also. Once an API has been
     * marked as deprecated, new APIs replacing them (if need be) would be in stable state for users
     * to migrate to.
     */
    DEPRECATED
}
