package com.aliyun.dts.deliver.protocol.transform;


import com.google.common.collect.ImmutableMap;

import java.util.Map;
import com.aliyun.dts.deliver.protocol.transform.JsonSchemaPrimitiveUtil.JsonSchemaPrimitive;
import static com.aliyun.dts.deliver.protocol.transform.JsonSchemaPrimitiveUtil.PRIMITIVE_TO_REFERENCE_BIMAP;

/**
 * Represents an Dts type. This corresponds to the data type that is present on the various
 * DtsMessages (e.g. DtsRecordMessage, DtsCatalog).
 *
 * This type system is realized using JSON schemas. In order to work around some of the limitations
 * of JSON schema, the newer version of the protocol defines new types in well_known_types.yaml.
 *
 * Note that the legacy version of the protocol relied on an dts_type property in the JSON
 * schema. This is NOT to be confused with the overall concept of an Dts data types, which is
 * essentially Dts's notion of what a record's data type is.
 *
 * TODO : Rename this file to DtsDataType.
 */
public class JsonSchemaType {

    public static final String TYPE = "type";
    public static final String REF = "$ref";
    public static final String FORMAT = "format";
    public static final String DATE_TIME = "date-time";
    public static final String DATE = "date";
    public static final String TIME = "time";
    public static final String TIME_WITHOUT_TIMEZONE = "time_without_timezone";
    public static final String TIME_WITH_TIMEZONE = "time_with_timezone";
    public static final String TIMESTAMP_WITH_TIMEZONE = "timestamp_with_timezone";
    public static final String TIMESTAMP_WITHOUT_TIMEZONE = "timestamp_without_timezone";
    public static final String AIRYBTE_INT_TYPE = "integer";
    public static final String CONTENT_ENCODING = "contentEncoding";
    public static final String BASE_64 = "base64";
    public static final String LEGACY_AIRBYTE_TYPE_PROPERTY = "dts_type";
    public static final String ITEMS = "items";
    public static final JsonSchemaType OBJECT = JsonSchemaType.builder(JsonSchemaPrimitiveUtil.JsonSchemaPrimitive.OBJECT).build();


    private final Map<String, Object> jsonSchemaTypeMap;

    private JsonSchemaType(final Map<String, Object> jsonSchemaTypeMap) {
        this.jsonSchemaTypeMap = jsonSchemaTypeMap;
    }

    public Map<String, Object> getJsonSchemaTypeMap() {
        return jsonSchemaTypeMap;
    }


    public static Builder builder(final JsonSchemaPrimitive type) {
        return new Builder(type);
    }

    public static class Builder {
        private final ImmutableMap.Builder<String, Object> typeMapBuilder;

        private Builder(final JsonSchemaPrimitive type) {
            typeMapBuilder = ImmutableMap.builder();
            if (JsonSchemaPrimitiveUtil.isV0Schema(type)) {
                typeMapBuilder.put(TYPE, type.name().toLowerCase());
            } else {
                typeMapBuilder.put(REF, PRIMITIVE_TO_REFERENCE_BIMAP.get(type));
            }
        }

        public JsonSchemaType build() {
            return new JsonSchemaType(typeMapBuilder.build());
        }
    }
}

