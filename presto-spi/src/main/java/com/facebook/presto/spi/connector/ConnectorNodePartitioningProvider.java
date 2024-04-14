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
package com.facebook.presto.spi.connector;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;

import java.util.List;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.util.Collections.singletonList;

/**
 * 理解这个方法的接口含义很关键：
 * {@link #getBucketCount}，{@link #getBucketNodeMap} 以及 {@link #getSplitBucketFunction} 主要
 * 是针对connector split调度相关（即split由那个节点负责数据读取）。其中，{@link #getBucketCount}数量和
 * connector本身的数据组织有关。
 *
 * {@link #getBucketFunction} 则和connector split的结果输出有关，即将结果划分到特定的输出bucket，具体的划分
 * 方法由{@link PartitioningScheme#partitioning}结合bucketCount来确定。而bucket对应到下游那个partition，
 * 则由{@link PartitioningScheme#bucketToPartition}控制。但上游stage输出使用的bucketToPartition则是下游
 * stage设置的，参考{@link SectionExecutionFactory#createStreamingLinkedStageExecutions}。通常情况下，
 * 下游stage会为每个partition创建一个任务，来消费上游的输出结果。
 *
 */
public interface ConnectorNodePartitioningProvider
{
    // TODO: Use ConnectorPartitionHandle (instead of int) to represent individual buckets.
    // Currently, it's mixed. listPartitionHandles used CPartitionHandle whereas the other functions used int.

    /**
     * Returns a list of all partitions associated with the provided {@code partitioningHandle}.
     * <p>
     * This method must be implemented for connectors that support addressable split discovery.
     * The partitions return here will be used as address for the purpose of split discovery.
     */
    /**
     * 本质上可以认为{@link ConnectorPartitionHandle}是对单个bucket的封装，当然一个ConnectorPartitionHandle也可以
     * 对应多个bucket，但当remote source不为REPLICATE时，则会break先有的限制性检查。
     *
     * 可以参考{@link com.facebook.presto.execution.scheduler.SectionExecutionFactory#createStageScheduler}
     * 中如何使用{@link #listPartitionHandles}的返回值。
     */
    default List<ConnectorPartitionHandle> listPartitionHandles(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return singletonList(NOT_PARTITIONED);
    }

    ConnectorBucketNodeMap getBucketNodeMap(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Node> sortedNodes);

    ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle);

    int getBucketCount(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle);

    /**
     * 和Connector的输出划分bucket相关，参考{@link com.facebook.presto.sql.planner.NodePartitioningManager#getPartitionFunction}。
     * 一般情况下，通过{@link com.facebook.presto.sql.planner.optimizations.AddExchanges.Rewriter#createPartitioning}为exchange
     * 来创建partitioning（默认情况下，采用的是SystemPartitioningHandle）。
     *
     * bucketed join场景下, 当下游的table定义自己的partitioning时（即split如何调度），上游stage的结果输出到具体哪个partition，则需要通过下
     * 面的函数来确定每行结果（即Page中的某个position）对应的bucket，然后再结合{@link PartitioningScheme#bucketToPartition}来确定对应的
     * partition（即输出到那个output buffer)。 参考: sql-samples/sqls-join-bucketed
     *
     * 而下游stage的任务创建的时候，会指定需要消费上游stage的哪个partition（即通过taskId的形式来指定）。而具体partition的数量, 则是由下游stage
     * 的执行并发度决定的(即有多少个tasks).
     */
    BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount);
}
