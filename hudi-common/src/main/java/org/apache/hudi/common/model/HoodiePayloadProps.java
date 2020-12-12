package org.apache.hudi.common.model;

/**
 * Since both payload classes and HoodiePayloadConfig needs to access these props, storing it here.
 */
public class HoodiePayloadProps {

  // payload ordering field
  public static final String PAYLOAD_ORDERING_FIELD_PROP = "hoodie.payload.ordering.field";
  public static String DEFAULT_PAYLOAD_ORDERING_FIELD_VAL = "ts";

}
