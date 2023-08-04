/*
 * Copyright (c) 2022 Dts, Inc., all rights reserved.
 */

package com.aliyun.dts.deliver.protocol.transform;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class JsonSchemaPrimitiveUtil {

  public enum JsonSchemaPrimitive {
    // V0 schema primitives
    STRING,
    NUMBER,
    OBJECT,
    ARRAY,
    BOOLEAN,
    NULL,
    // V1 schema primitives
    STRING_V1,
    BINARY_DATA_V1,
    DATE_V1,
    TIMESTAMP_WITH_TIMEZONE_V1,
    TIMESTAMP_WITHOUT_TIMEZONE_V1,
    TIME_WITH_TIMEZONE_V1,
    TIME_WITHOUT_TIMEZONE_V1,
    NUMBER_V1,
    INTEGER_V1,
    BOOLEAN_V1;
  }

  public static final Set<JsonSchemaPrimitive> VO_JSON_SCHEMA_PRIMITIVE_SET =
      ImmutableSet.of(
          JsonSchemaPrimitive.STRING,
          JsonSchemaPrimitive.NUMBER,
          JsonSchemaPrimitive.OBJECT,
          JsonSchemaPrimitive.ARRAY,
          JsonSchemaPrimitive.BOOLEAN,
          JsonSchemaPrimitive.NULL);

  public static final boolean isV0Schema(final JsonSchemaPrimitive type) {
    return VO_JSON_SCHEMA_PRIMITIVE_SET.contains(type);
  }

  public static final BiMap<JsonSchemaPrimitive, String> PRIMITIVE_TO_REFERENCE_BIMAP =
          new ImmutableBiMap.Builder<JsonSchemaPrimitive, String>()
                  .build();

}
