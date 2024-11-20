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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.BucketPartitionFunction;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PipelineExecutionStrategy;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.SystemPartitioningHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.operator.exchange.LocalExchangeSink.finishedLocalExchangeSink;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LocalExchange
{
    private final Supplier<LocalExchanger> exchangerSupplier;

    private final List<LocalExchangeSource> sources;

    private final LocalExchangeMemoryManager memoryManager;

    @GuardedBy("this")
    private boolean allSourcesFinished;

    @GuardedBy("this")
    private final List<LocalExchangeSinkFactory> allSinkFactories;

    @GuardedBy("this")
    private final Set<LocalExchangeSinkFactory> openSinkFactories = new HashSet<>();

    @GuardedBy("this")
    private final Set<LocalExchangeSink> sinks = new HashSet<>();

    @GuardedBy("this")
    private int nextSourceIndex;

    public LocalExchange(
            PartitioningProviderManager partitioningProviderManager,
            Session session,
            int sinkFactoryCount,
            int bufferCount,
            PartitioningHandle partitioning,
            List<Integer> partitionChannels,
            List<Type> partitioningChannelTypes,
            Optional<Integer> partitionHashChannel,
            DataSize maxBufferedBytes)
    {
        /**
         * local {@link com.facebook.presto.sql.planner.plan.ExchangeNode#sources}的大小可以超过1,
         * 此时每个source会有自己独立的LocalExchangeSinkFactory.
         */
        this.allSinkFactories = Stream.generate(() -> new LocalExchangeSinkFactory(LocalExchange.this))
                .limit(sinkFactoryCount)
                .collect(toImmutableList());
        openSinkFactories.addAll(allSinkFactories);

        ImmutableList.Builder<LocalExchangeSource> sources = ImmutableList.builder();
        for (int i = 0; i < bufferCount; i++) {
            sources.add(new LocalExchangeSource(source -> checkAllSourcesFinished()));
        }
        this.sources = sources.build();

        List<Consumer<PageReference>> buffers = this.sources.stream()
                .map(buffer -> (Consumer<PageReference>) buffer::addPage)
                .collect(toImmutableList());

        this.memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            exchangerSupplier = () -> new RandomExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
            /**
             * 当LocalExchange需要进行归并排序时, 会用到FIXED_PASSTHROUGH_DISTRIBUTION, 比如task中的pipelines:
             * 1）TaskOutputOperator -> LocalMergeSourceOperator (drivers: 1)
             * 2) LocalExchangeSinkOperator -> OrderByOperator -> ExchangeOperator (drivers: 4)
             *
             * LocalExchangeSinkOperator所在pipeline的每个stream（即每个driver的输出）满足排序, 这里需要保证每个
             * stream一一对应到一个LocalExchangeSource (LocalExecutionPlanner会保证使用的LocalExchangeSource
             * 数量和LocalExchangeSinkOperator所在pipeline的drivers个数一致)。这样LocalMergeSourceOperator可以
             * 采用单个driver将多个LocalExchangeSource的结果进行归并排序后输出。
             *
             * 说明:
             * 不会存在如下pipeline, 因为LocalExchangeSinkOperator参与的drivers个数是不确定的, 下面的逻辑会break.
             * AddLocalExchanges保证在distributed_sort开启的时候, 在SortNode之前插入remote ExchangeNode.
             * 1）TaskOutputOperator -> LocalMergeSourceOperator (drivers: 1)
             * 2) LocalExchangeSinkOperator -> OrderByOperator -> TableScanOperator (drivers: per split)
             *
             * 参考:
             * {@link com.facebook.presto.sql.planner.optimizations.AddLocalExchanges.Rewriter#visitSort}
             * {@link com.facebook.presto.sql.planner.LocalExecutionPlanner.Visitor#createLocalMerge}
             * sql-samples/sqls-sort-merge
             */
            Iterator<LocalExchangeSource> sourceIterator = this.sources.iterator();
            exchangerSupplier = () -> {
                checkState(sourceIterator.hasNext(), "no more sources");
                return new PassthroughExchanger(sourceIterator.next(), maxBufferedBytes.toBytes() / bufferCount, memoryManager::updateMemoryUsage);
            };
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getConnectorId().isPresent()) {
            /**
             * 看起来所有的sinks理论上可以复用同一个PartitioningExchanger, 因为{@link PartitioningExchanger#accept(Page)}
             * 是线程安全的. 但这样的话, PartitioningExchanger#accept会存在多线程竞争同步锁.
             */
            exchangerSupplier = () -> new PartitioningExchanger(
                    buffers,
                    memoryManager,
                    createPartitionFunction(
                            partitioningProviderManager,
                            session,
                            partitioning,
                            bufferCount,
                            partitioningChannelTypes,
                            partitionHashChannel.isPresent()),
                    partitionChannels,
                    partitionHashChannel);
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
    }

    static PartitionFunction createPartitionFunction(
            PartitioningProviderManager partitioningProviderManager,
            Session session,
            PartitioningHandle partitioning,
            int partitionCount,
            List<Type> partitioningChannelTypes,
            boolean isHashPrecomputed)
    {
        /**
         * 通过{@link com.facebook.presto.sql.planner.optimizations.AddLocalExchanges.Rewriter#enforce}可以知道,
         * LOCAL ExchangeNode采用的partitioning方式都是{@link SystemPartitioningHandle}.
         */
        if (partitioning.getConnectorHandle() instanceof SystemPartitioningHandle) {
            HashGenerator hashGenerator = isHashPrecomputed
                    ? new PrecomputedHashGenerator(0)
                    : InterpretedHashGenerator.createPositionalWithTypes(partitioningChannelTypes);
            return new LocalPartitionGenerator(hashGenerator, partitionCount);
        }

        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviderManager.getPartitioningProvider(partitioning.getConnectorId().get());
        int bucketCount = partitioningProvider.getBucketCount(
                partitioning.getTransactionHandle().orElse(null),
                session.toConnectorSession(partitioning.getConnectorId().get()),
                partitioning.getConnectorHandle());
        int[] bucketToPartition = new int[bucketCount];
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            bucketToPartition[bucket] = bucket % partitionCount;
        }

        BucketFunction bucketFunction = partitioningProvider.getBucketFunction(
                partitioning.getTransactionHandle().orElse(null),
                session.toConnectorSession(partitioning.getConnectorId().get()),
                partitioning.getConnectorHandle(),
                partitioningChannelTypes,
                bucketCount);

        checkArgument(bucketFunction != null, "No bucket function for partitioning: %s", partitioning);
        return new BucketPartitionFunction(bucketFunction, bucketToPartition);
    }

    public int getBufferCount()
    {
        return sources.size();
    }

    public long getBufferedBytes()
    {
        return memoryManager.getBufferedBytes();
    }

    public synchronized LocalExchangeSinkFactory getSinkFactory(LocalExchangeSinkFactoryId id)
    {
        return allSinkFactories.get(id.id);
    }

    public synchronized LocalExchangeSource getNextSource()
    {
        checkState(nextSourceIndex < sources.size(), "All operators already created");
        LocalExchangeSource result = sources.get(nextSourceIndex);
        nextSourceIndex++;
        return result;
    }

    public LocalExchangeSource getSource(int partitionIndex)
    {
        return sources.get(partitionIndex);
    }

    private void checkAllSourcesFinished()
    {
        checkNotHoldsLock(this);

        if (!sources.stream().allMatch(LocalExchangeSource::isFinished)) {
            return;
        }

        // all sources are finished, so finish the sinks
        ImmutableList<LocalExchangeSink> openSinks;
        synchronized (this) {
            allSourcesFinished = true;

            openSinks = ImmutableList.copyOf(sinks);
            sinks.clear();
        }

        // since all sources are finished there is no reason to allow new pages to be added
        // this can happen with a limit query
        openSinks.forEach(LocalExchangeSink::finish);
        checkAllSinksComplete();
    }

    private LocalExchangeSink createSink(LocalExchangeSinkFactory factory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            checkState(openSinkFactories.contains(factory), "Factory is already closed");

            if (allSourcesFinished) {
                // all sources have completed so return a sink that is already finished
                return finishedLocalExchangeSink();
            }

            // Note: exchanger can be stateful so create a new one for each sink
            LocalExchanger exchanger = exchangerSupplier.get();
            LocalExchangeSink sink = new LocalExchangeSink(exchanger, this::sinkFinished);
            sinks.add(sink);
            return sink;
        }
    }

    /**
     * 当{@link LocalExchangeSinkOperator#close()}调用时（即对应的driver退出时），将会调用这个方法。
     *
     * {@link LocalExchangeSink}和{@link LocalExchangeSinkOperator}一一对应。
     */
    private void sinkFinished(LocalExchangeSink sink)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            sinks.remove(sink);
        }
        checkAllSinksComplete();
    }

    /**
     * 当{@link LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory#noMoreOperators()}将会调用这个方法，这个发生在对应
     * 的pipeline不再需要创建新的Drivers（即{@link com.facebook.presto.operator.DriverFactory#noMoreDrivers()}）。
     */
    private void sinkFactoryClosed(LocalExchangeSinkFactory sinkFactory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            openSinkFactories.remove(sinkFactory);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            if (!openSinkFactories.isEmpty() || !sinks.isEmpty()) {
                return;
            }
        }

        sources.forEach(LocalExchangeSource::finish);
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Can not execute this method while holding a lock");
    }

    @ThreadSafe
    public static class LocalExchangeFactory
    {
        private final PartitioningProviderManager partitioningProviderManager;
        private final Session session;
        private final PartitioningHandle partitioning;
        private final List<Integer> partitionChannels;
        private final List<Type> partitioningChannelTypes;
        private final Optional<Integer> partitionHashChannel;
        private final PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy;
        private final DataSize maxBufferedBytes;
        private final int bufferCount;

        @GuardedBy("this")
        private boolean noMoreSinkFactories;
        // The number of total sink factories are tracked at planning time
        // so that the exact number of sink factory is known by the time execution starts.
        @GuardedBy("this")
        private int numSinkFactories;

        @GuardedBy("this")
        private final Map<Lifespan, LocalExchange> localExchangeMap = new HashMap<>();
        @GuardedBy("this")
        private final List<LocalExchangeSinkFactoryId> closedSinkFactories = new ArrayList<>();

        public LocalExchangeFactory(
                PartitioningProviderManager partitioningProviderManager,
                Session session,
                PartitioningHandle partitioning,
                int defaultConcurrency,
                List<Type> types,
                List<Integer> partitionChannels,
                Optional<Integer> partitionHashChannel,
                PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy,
                DataSize maxBufferedBytes)
        {
            this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
            this.session = requireNonNull(session, "session is null");
            this.partitioning = requireNonNull(partitioning, "partitioning is null");
            this.bufferCount = computeBufferCount(partitioning, defaultConcurrency, partitionChannels);
            this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
            requireNonNull(types, "types is null");
            this.partitioningChannelTypes = partitionChannels.stream()
                    .map(types::get)
                    .collect(toImmutableList());
            this.partitionHashChannel = requireNonNull(partitionHashChannel, "partitionHashChannel is null");
            this.exchangeSourcePipelineExecutionStrategy = requireNonNull(exchangeSourcePipelineExecutionStrategy, "exchangeSourcePipelineExecutionStrategy is null");
            this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");
        }

        public synchronized LocalExchangeSinkFactoryId newSinkFactoryId()
        {
            checkState(!noMoreSinkFactories);
            LocalExchangeSinkFactoryId result = new LocalExchangeSinkFactoryId(numSinkFactories);
            numSinkFactories++;
            return result;
        }

        public synchronized void noMoreSinkFactories()
        {
            noMoreSinkFactories = true;
        }

        public int getBufferCount()
        {
            return bufferCount;
        }

        public synchronized LocalExchange getLocalExchange(Lifespan lifespan)
        {
            if (exchangeSourcePipelineExecutionStrategy == UNGROUPED_EXECUTION) {
                checkArgument(lifespan.isTaskWide(), "LocalExchangeFactory is declared as UNGROUPED_EXECUTION. Driver-group exchange cannot be created.");
            }
            else {
                checkArgument(!lifespan.isTaskWide(), "LocalExchangeFactory is declared as GROUPED_EXECUTION. Task-wide exchange cannot be created.");
            }
            return localExchangeMap.computeIfAbsent(lifespan, ignored -> {
                checkState(noMoreSinkFactories);
                LocalExchange localExchange = new LocalExchange(
                        partitioningProviderManager,
                        session,
                        numSinkFactories,
                        bufferCount,
                        partitioning,
                        partitionChannels,
                        partitioningChannelTypes,
                        partitionHashChannel,
                        maxBufferedBytes);
                for (LocalExchangeSinkFactoryId closedSinkFactoryId : closedSinkFactories) {
                    localExchange.getSinkFactory(closedSinkFactoryId).close();
                }
                return localExchange;
            });
        }

        /**
         * 对应的sinkFactory不会再产生新的sinks, 即对应的LocalExchangeSinkOperatorFactory已经完成
         * 所在pipeline的所有Driver实例的LocalExchangeSinkOperator的生成.
         */
        public synchronized void closeSinkFactory(LocalExchangeSinkFactoryId sinkFactoryId)
        {
            closedSinkFactories.add(sinkFactoryId);
            for (LocalExchange localExchange : localExchangeMap.values()) {
                localExchange.getSinkFactory(sinkFactoryId).close();
            }
        }
    }

    private static int computeBufferCount(PartitioningHandle partitioning, int defaultConcurrency, List<Integer> partitionChannels)
    {
        int bufferCount;
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            bufferCount = 1;
            checkArgument(partitionChannels.isEmpty(), "Gather exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Broadcast exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Arbitrary exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Passthrough exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getConnectorId().isPresent()) {
            // partitioned exchange
            bufferCount = defaultConcurrency;
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
        return bufferCount;
    }

    public static class LocalExchangeSinkFactoryId
    {
        private final int id;

        public LocalExchangeSinkFactoryId(int id)
        {
            this.id = id;
        }
    }

    // Sink factory is entirely a pass through to LocalExchange.
    // This class only exists as a separate entity to deal with the complex lifecycle caused
    // by operator factories.
    @ThreadSafe
    public static class LocalExchangeSinkFactory
            implements Closeable
    {
        private final LocalExchange exchange;

        private LocalExchangeSinkFactory(LocalExchange exchange)
        {
            this.exchange = requireNonNull(exchange, "exchange is null");
        }

        public LocalExchangeSink createSink()
        {
            return exchange.createSink(this);
        }

        @Override
        public void close()
        {
            exchange.sinkFactoryClosed(this);
        }
    }
}
