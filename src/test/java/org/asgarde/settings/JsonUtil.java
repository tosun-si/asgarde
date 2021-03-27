package org.asgarde.settings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.InputStream;
import java.util.List;

public class JsonUtil {
    public static final ObjectMapper OBJECT_MAPPER;

    static {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false);

        OBJECT_MAPPER = mapper;
    }

    /**
     * Maps the given generic object to a JSON string.
     */
    public static <T> String serialize(final T obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("The serialization of object fails : " + obj);
        }
    }

    /**
     * Serializes a JSON string to an object of given type.
     */
    public static <T> T deserialize(final String json, final Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("The deserialization of Json string fails " + json);
        }
    }

    /**
     * Deserializes a JSON file present in the resource (classpath) to a Seq of the given object.
     */
    public static <T> List<T> deserializeFromResourcePath(final String resourcePath, final TypeReference<List<T>> reference) {
        final InputStream stream = JsonUtil.class
                .getClassLoader()
                .getResourceAsStream(resourcePath);

        return deserializeToList(stream, reference);
    }

    /**
     * Deserializes a JSON string to a Seq of the given object.
     */
    public static <T> List<T> deserializeToList(final InputStream stream, final TypeReference<List<T>> reference) {
        try {
            return OBJECT_MAPPER.readValue(stream, reference);
        } catch (Exception e) {
            throw new IllegalStateException("The deserialization of inputStream for the following reference type " + reference);
        }
    }
}
