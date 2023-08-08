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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ElasticsearchColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type type;
    private final DecoderDescriptor decoderDescriptor;
    private final ColumnPredicateSupport columnPredicateSupport;
    private final List<String> dereferenceNames;
    private final String qualifiedName;

    @JsonCreator
    public ElasticsearchColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("decoderDescriptor") DecoderDescriptor decoderDescriptor,
            @JsonProperty("columnPredicateSupport") ColumnPredicateSupport columnPredicateSupport,
            @JsonProperty("dereferenceNames") List<String> dereferenceNames)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.decoderDescriptor = requireNonNull(decoderDescriptor, "decoderDescriptor is null");
        this.columnPredicateSupport = columnPredicateSupport;
        this.dereferenceNames = requireNonNull(dereferenceNames, "dereferenceNames is null");
        this.qualifiedName = buildQualifiedName();
    }

    private String buildQualifiedName() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.name);
        if (dereferenceNames.size() > 0) {
            builder.append(".");
            builder.append(String.join(".", this.dereferenceNames));
        }
        return builder.toString();
    }

    public String getQualifiedName() {
        return this.qualifiedName;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public DecoderDescriptor getDecoderDescriptor()
    {
        return decoderDescriptor;
    }

    @JsonProperty
    public ColumnPredicateSupport getColumnPredicateSupport()
    {
        return columnPredicateSupport;
    }

    @JsonProperty
    public List<String> getDereferenceNames() {
        return dereferenceNames;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, decoderDescriptor, columnPredicateSupport, dereferenceNames);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ElasticsearchColumnHandle other = (ElasticsearchColumnHandle) obj;
        return Objects.equals(this.getColumnPredicateSupport(), other.getColumnPredicateSupport()) &&
                Objects.equals(this.getName(), other.getName()) &&
                Objects.equals(this.getType(), other.getType()) &&
                Objects.equals(this.getDecoderDescriptor(), other.getDecoderDescriptor()) &&
                Objects.equals(this.getDereferenceNames(), other.getDereferenceNames());
    }

    @Override
    public String toString()
    {
        return getQualifiedName() + "::" + getType();
    }
}
