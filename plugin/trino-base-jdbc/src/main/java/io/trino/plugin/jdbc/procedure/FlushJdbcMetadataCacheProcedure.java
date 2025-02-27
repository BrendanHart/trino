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
package io.trino.plugin.jdbc.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.base.mapping.CachingIdentifierMapping;
import io.trino.plugin.jdbc.CachingJdbcClient;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.plugin.base.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class FlushJdbcMetadataCacheProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle FLUSH_JDBC_METADATA_CACHE = methodHandle(FlushJdbcMetadataCacheProcedure.class, "flushMetadataCache");

    private final CachingJdbcClient cachingJdbcClient;
    private final Optional<CachingIdentifierMapping> cachingIdentifierMapping;

    @Inject
    public FlushJdbcMetadataCacheProcedure(
            CachingJdbcClient cachingJdbcClient,
            Optional<CachingIdentifierMapping> cachingIdentifierMapping)
    {
        this.cachingJdbcClient = requireNonNull(cachingJdbcClient, "cachingJdbcClient is null");
        this.cachingIdentifierMapping = requireNonNull(cachingIdentifierMapping, "cachingIdentifierMapping is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "flush_metadata_cache",
                ImmutableList.of(),
                FLUSH_JDBC_METADATA_CACHE.bindTo(this));
    }

    public void flushMetadataCache()
    {
        cachingJdbcClient.flushCache();
        cachingIdentifierMapping.ifPresent(CachingIdentifierMapping::flushCache);
    }
}
