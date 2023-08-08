/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class ElasticsearchTableHandle
        implements ConnectorTableHandle
{
    public enum Type
    {
        SCAN, QUERY
    }

    private final Type type;
    private final String schema;
    private final String index;
    private final TupleDomain<ColumnHandle> constraint;
    private final Map<String, String> regexes;
    private final Optional<String> query;
    private final OptionalLong limit;
    private final Set<ElasticsearchColumnHandle> projectedColumns;

    public ElasticsearchTableHandle(Type type, String schema, String index, Optional<String> query)
    {
        this.type = requireNonNull(type, "type is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.index = requireNonNull(index, "index is null");
        this.query = requireNonNull(query, "query is null");

        constraint = TupleDomain.all();
        regexes = ImmutableMap.of();
        limit = OptionalLong.empty();
        projectedColumns = ImmutableSet.of();
    }

    @JsonCreator
    public ElasticsearchTableHandle(
            @JsonProperty("type") Type type,
            @JsonProperty("schema") String schema,
            @JsonProperty("index") String index,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("regexes") Map<String, String> regexes,
            @JsonProperty("query") Optional<String> query,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("projectedColumns") Set<ElasticsearchColumnHandle> projectedColumns)
    {
        this.type = requireNonNull(type, "type is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.index = requireNonNull(index, "index is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.regexes = ImmutableMap.copyOf(requireNonNull(regexes, "regexes is null"));
        this.query = requireNonNull(query, "query is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
    }

    public ElasticsearchTableHandle withProjectedColumns(Set<ElasticsearchColumnHandle> projectedColumns) {
        return new ElasticsearchTableHandle(
                type,
                schema,
                index,
                constraint,
                regexes,
                query,
                limit,
                projectedColumns
        );
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Map<String, String> getRegexes()
    {
        return regexes;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return query;
    }

    @JsonProperty
    public Set<ElasticsearchColumnHandle> getProjectedColumns() {
        return projectedColumns;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchTableHandle that = (ElasticsearchTableHandle) o;
        return type == that.type &&
                schema.equals(that.schema) &&
                index.equals(that.index) &&
                constraint.equals(that.constraint) &&
                regexes.equals(that.regexes) &&
                query.equals(that.query) &&
                limit.equals(that.limit) &&
                projectedColumns.equals(that.projectedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, schema, index, constraint, regexes, query, limit, projectedColumns);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(type + ":" + index);

        StringBuilder attributes = new StringBuilder();
        if (!regexes.isEmpty()) {
            attributes.append("regexes=[");
            attributes.append(regexes.entrySet().stream()
                    .map(regex -> regex.getKey() + ":" + regex.getValue())
                    .collect(Collectors.joining(", ")));
            attributes.append("]");
        }
        limit.ifPresent(value -> attributes.append("limit=" + value));
        query.ifPresent(value -> attributes.append("query" + value));

        if (attributes.length() > 0) {
            builder.append("(");
            builder.append(attributes);
            builder.append(")");
        }

        return builder.toString();
    }
}
