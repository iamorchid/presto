package com.facebook.presto.plugin.memory;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;

public class MemoryNodePartitioningProvider
        implements ConnectorNodePartitioningProvider {
    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Node> sortedNodes) {
        checkState(!sortedNodes.isEmpty(), "no available nodes");

        /**
         * 对于写和读来说，它们使用的bucket和node映射必须一致（这里采用固定映射），否则写进去的数据，就无法读取到。
         */
        int buckets = getBucketCount(transactionHandle, session, partitioningHandle);
        List<Node> bucketToNode = new ArrayList<>(buckets);
        for (int bucket = 0; bucket < buckets; ++bucket) {
            bucketToNode.add(sortedNodes.get(bucket % sortedNodes.size()));
        }
        return ConnectorBucketNodeMap.createBucketNodeMap(bucketToNode, NodeSelectionStrategy.HARD_AFFINITY);
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle) {
        return split -> ((MemorySplit) split).getBucket();
    }

    @Override
    public int getBucketCount(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle) {
        return ((MemoryPartitioningHandle) partitioningHandle).getBucketProperty().getBuckets();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount) {
        MemoryPartitioningHandle handle = (MemoryPartitioningHandle) partitioningHandle;
        checkState(handle.getBucketProperty().getBucketedBy().size() == partitionChannelTypes.size(), "inconsistent partitioning columns");
        checkState(handle.getBucketProperty().getBuckets() == bucketCount, "inconsistent bucket count");

        return new MemoryBucketFunction(bucketCount, partitionChannelTypes);
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle) {
        int buckets = getBucketCount(transactionHandle, session, partitioningHandle);
        return IntStream.range(0, buckets).mapToObj(MemoryPartitionHandle::new).collect(Collectors.toList());
    }

}
