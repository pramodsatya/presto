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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterServiceFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestNativeExpressionsPlugin
{
    @Test
    public void testLoadNativeExpressionsPlugin()
    {
        Plugin plugin = new NativeExpressionsPlugin();
        Iterable<RowExpressionInterpreterServiceFactory> serviceFactories = plugin.getBatchRowExpressionInterpreterProviders();
        RowExpressionInterpreterServiceFactory factory = getOnlyElement(serviceFactories);
        factory.createInterpreter(ImmutableMap.of(), new DummyNodeManager(), new DummyRowExpressionSerde());
    }

    private static class DummyNodeManager
            implements NodeManager
    {
        @Override
        public Set<Node> getAllNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Node> getWorkerNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Node getCurrentNode()
        {
            return new DummyNode();
        }

        @Override
        public String getEnvironment()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class DummyRowExpressionSerde
            implements RowExpressionSerde
    {
        @Override
        public byte[] serialize(RowExpression expression)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowExpression deserialize(byte[] data)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class DummyNode
            implements Node
    {
        @Override
        public String getHost()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public HostAddress getHostAndPort()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getHttpUri()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getNodeIdentifier()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getVersion()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCoordinator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isResourceManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCatalogServer()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public NodePoolType getPoolType()
        {
            throw new UnsupportedOperationException();
        }
    }
}
