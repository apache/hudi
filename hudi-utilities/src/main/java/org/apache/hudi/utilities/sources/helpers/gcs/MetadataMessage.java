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

package org.apache.hudi.utilities.sources.helpers.gcs;

import com.google.pubsub.v1.PubsubMessage;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.sources.helpers.gcs.MessageValidity.ProcessingDecision.DO_SKIP;

/**
 * Wraps a PubsubMessage assuming it's from Cloud Storage Pubsub Notifications (CSPN), and
 * adds relevant helper methods.
 * For details of CSPN messages see: https://cloud.google.com/storage/docs/pubsub-notifications
 */
public class MetadataMessage {

  // The CSPN message to wrap
  private final PubsubMessage message;

  private static final String EVENT_NAME_OBJECT_FINALIZE = "OBJECT_FINALIZE";

  private static final String ATTR_EVENT_TYPE = "eventType";
  private static final String ATTR_OBJECT_ID = "objectId";
  private static final String ATTR_OVERWROTE_GENERATION = "overwroteGeneration";

  public MetadataMessage(PubsubMessage message) {
    this.message = message;
  }

  public String toStringUtf8() {
    return message.getData().toStringUtf8();
  }

  /**
   * Whether a message is valid to be ingested and stored by this Metadata puller.
   * Ref: https://cloud.google.com/storage/docs/pubsub-notifications#events
   */
  public MessageValidity shouldBeProcessed() {
    if (!isNewFileCreation()) {
      return new MessageValidity(DO_SKIP, "eventType: " + getEventType() + ". Not a file creation message.");
    }

    if (isOverwriteOfExistingFile()) {
      return new MessageValidity(DO_SKIP,
      "eventType: " + getEventType()
              + ". Overwrite of existing objectId: " + getObjectId()
              + " with generation numner: " + getOverwroteGeneration()
      );
    }

    return MessageValidity.DEFAULT_VALID_MESSAGE;
  }

  /**
   * Whether message represents an overwrite of an existing file.
   * Ref: https://cloud.google.com/storage/docs/pubsub-notifications#replacing_objects
   */
  private boolean isOverwriteOfExistingFile() {
    return !isNullOrEmpty(getOverwroteGeneration());
  }

  /**
   * Returns true if message corresponds to new file creation, false if not.
   * Ref: https://cloud.google.com/storage/docs/pubsub-notifications#events
   */
  private boolean isNewFileCreation() {
    return EVENT_NAME_OBJECT_FINALIZE.equals(getEventType());
  }

  public String getEventType() {
    return getAttr(ATTR_EVENT_TYPE);
  }

  public String getObjectId() {
    return getAttr(ATTR_OBJECT_ID);
  }

  public String getOverwroteGeneration() {
    return getAttr(ATTR_OVERWROTE_GENERATION);
  }

  private String getAttr(String attrName) {
    return message.getAttributesMap().get(attrName);
  }

}
