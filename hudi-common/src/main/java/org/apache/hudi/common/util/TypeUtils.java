package org.apache.hudi.common.util;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TypeUtils {

  private TypeUtils() {}

  /**
   * Maps values from the provided Enum's {@link Class} into corresponding values,
   * extracted by provided {@code valueMapper}
   */
  public static <EnumT extends Enum<EnumT>> Map<String, EnumT> getValueToEnumMap(
      @Nonnull Class<EnumT> klass,
      @Nonnull Function<EnumT, String> valueMapper
  ) {
    return Arrays.stream(klass.getEnumConstants())
        .collect(Collectors.toMap(valueMapper, Function.identity()));
  }

}
