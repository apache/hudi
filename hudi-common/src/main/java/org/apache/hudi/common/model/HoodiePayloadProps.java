package org.apache.hudi.common.model;

/**
 * Holds payload properties that implementation of {@link HoodieRecordPayload} can leverage.
 * Since both payload classes and HoodiePayloadConfig needs to access these props, storing it here in hudi-common.
 */
public class HoodiePayloadProps {

  // payload ordering field. This could be used to merge incoming record with that in storage. Implementations of
  // {@link HoodieRecordPayload} can leverage if required.
  public static final String PAYLOAD_ORDERING_FIELD_PROP = "hoodie.payload.ordering.field";
  public static String DEFAULT_PAYLOAD_ORDERING_FIELD_VAL = "ts";

}
