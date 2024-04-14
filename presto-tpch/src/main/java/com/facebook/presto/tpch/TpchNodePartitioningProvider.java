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
package com.facebook.presto.tpch;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Collections.singletonList;

public class TpchNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public TpchNodePartitioningProvider(NodeManager nodeManager, int splitsPerNode)
    {
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Node> sortedNodes)
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();

        // Split the data using split and skew by the number of nodes available.
        return createBucketNodeMap(toIntExact((long) nodes.size() * splitsPerNode));
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((TpchSplit) value).getPartNumber();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        /**
         * 通过列orderKey来判断某行所属的bucket（表orders以及lineitem都包含orderKey列）。
         */
        long totalRows = ((TpchPartitioningHandle) partitioningHandle).getTotalRows();
        long rowsPerBucket = totalRows / bucketCount;
        checkArgument(partitionChannelTypes.equals(ImmutableList.of(BIGINT)), "Expected one BIGINT parameter");
        return new TpchBucketFunction(bucketCount, rowsPerBucket);
    }

    @Override
    public int getBucketCount(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return nodeManager.getRequiredWorkerNodes().size() * splitsPerNode;
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle) {
        if (((TpchPartitioningHandle) partitioningHandle).isGroupedExecutionDisabled()) {
            return singletonList(NOT_PARTITIONED);
        }

        int bucketCount = nodeManager.getRequiredWorkerNodes().size() * splitsPerNode;
        return IntStream.range(0, bucketCount).mapToObj(TpchPartitionHandle::new).collect(toImmutableList());
    }
}
