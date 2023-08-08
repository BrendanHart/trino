package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;

public class ColumnPredicateSupport {
    private final boolean supportsPredicates;
    private final Map<String, ColumnPredicateSupport> children;

    @JsonCreator
    public ColumnPredicateSupport(
            @JsonProperty("supportsPredicates") boolean supportsPredicates,
            @JsonProperty("children") Map<String, ColumnPredicateSupport> children) {
        this.supportsPredicates = supportsPredicates;
        this.children = ImmutableMap.copyOf(children);
    }

    @JsonProperty
    public Map<String, ColumnPredicateSupport> getChildren() {
        return children;
    }

    @JsonProperty
    public boolean isSupportsPredicates()
    {
        return supportsPredicates;
    }

    public static ColumnPredicateSupport createFromElasticsearchType(IndexMetadata.Type type) {
        Map<String, ColumnPredicateSupport> children = new HashMap<>();
        if (type instanceof IndexMetadata.ObjectType objectType) {
            children = objectType.getFields().stream().collect(Collectors.toMap(
                    IndexMetadata.Field::getName,
                    field -> createFromElasticsearchType(field.getType())
            ));
        }
        return new ColumnPredicateSupport(
                supportsPredicates(type),
                children
        );
    }

    public static ColumnPredicateSupport createFromBoolean(boolean supportsPredicates) {
        return new ColumnPredicateSupport(
                supportsPredicates,
                ImmutableMap.of()
        );
    }

    private static boolean supportsPredicates(IndexMetadata.Type type)
    {
        if (type instanceof IndexMetadata.DateTimeType) {
            return true;
        }

        if (type instanceof IndexMetadata.PrimitiveType) {
            switch (((IndexMetadata.PrimitiveType) type).getName().toLowerCase(ENGLISH)) {
                case "boolean":
                case "byte":
                case "short":
                case "integer":
                case "long":
                case "double":
                case "float":
                case "keyword":
                    return true;
            }
        }

        return false;
    }
}
