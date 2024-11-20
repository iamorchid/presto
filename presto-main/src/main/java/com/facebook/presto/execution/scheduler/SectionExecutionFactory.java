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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.Session;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PlanFragmenterUtils;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.facebook.presto.spi.ConnectorTablePartitioning;

import static com.facebook.presto.SystemSessionProperties.getConcurrentLifespansPerNode;
import static com.facebook.presto.SystemSessionProperties.getMaxTasksPerStage;
import static com.facebook.presto.SystemSessionProperties.getWriterMinSize;
import static com.facebook.presto.SystemSessionProperties.isOptimizedScaleWriterProducerBuffer;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.spi.ConnectorId.isInternalSystemConnector;
import static com.facebook.presto.spi.NodePoolType.INTERMEDIATE;
import static com.facebook.presto.spi.NodePoolType.LEAF;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class SectionExecutionFactory
{
    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeTaskMap nodeTaskMap;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final FailureDetector failureDetector;
    private final SplitSchedulerStats schedulerStats;
    private final NodeScheduler nodeScheduler;
    private final int splitBatchSize;
    private final boolean isEnableWorkerIsolation;

    @Inject
    public SectionExecutionFactory(
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            NodeTaskMap nodeTaskMap,
            @ForQueryExecution ExecutorService executor,
            @ForScheduler ScheduledExecutorService scheduledExecutor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            NodeScheduler nodeScheduler,
            QueryManagerConfig queryManagerConfig)
    {
        this(
                metadata,
                nodePartitioningManager,
                nodeTaskMap,
                executor,
                scheduledExecutor,
                failureDetector,
                schedulerStats,
                nodeScheduler,
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize(),
                queryManagerConfig.isEnableWorkerIsolation());
    }

    public SectionExecutionFactory(
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            NodeTaskMap nodeTaskMap,
            ExecutorService executor,
            ScheduledExecutorService scheduledExecutor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            NodeScheduler nodeScheduler,
            int splitBatchSize,
            boolean isEnableWorkerIsolation)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.splitBatchSize = splitBatchSize;
        this.isEnableWorkerIsolation = isEnableWorkerIsolation;
    }

    /**
     * returns a List of SectionExecutions in a postorder representation of the tree
     */
    public SectionExecution createSectionExecutions(
            Session session,
            StreamingPlanSection section,
            ExchangeLocationsConsumer locationsConsumer,
            Optional<int[]> bucketToPartition,
            OutputBuffers outputBuffers,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId)
    {
        // Only fetch a distribution once per section to ensure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();
        TableWriteInfo tableWriteInfo = createTableWriteInfo(section.getPlan(), metadata, session);
        Optional<Predicate<Node>> nodePredicate = getNodePoolSelectionPredicate(section.getPlan());
        List<StageExecutionAndScheduler> sectionStages = createStreamingLinkedStageExecutions(
                session,
                locationsConsumer,
                section.getPlan().withBucketToPartition(bucketToPartition),
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle, nodePredicate)),
                tableWriteInfo,
                Optional.empty(),
                summarizeTaskInfo,
                remoteTaskFactory,
                splitSourceFactory,
                attemptId);
        StageExecutionAndScheduler rootStage = getLast(sectionStages);
        rootStage.getStageExecution().setOutputBuffers(outputBuffers);
        return new SectionExecution(rootStage, sectionStages);
    }

    /**
     * returns a List of StageExecutionAndSchedulers in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createStreamingLinkedStageExecutions(
            Session session,
            ExchangeLocationsConsumer parent,
            StreamingSubPlan plan,
            /**
             * 这里采用cache是为了保证多次通过PartitioningHandle获取NodePartitionMap时，得到的bucketToNode，
             * bucketToPartition以及partitionToNode是稳定的。
             */
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            TableWriteInfo tableWriteInfo,
            Optional<SqlStageExecution> parentStageExecution,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId)
    {
        ImmutableList.Builder<StageExecutionAndScheduler> stageExecutionAndSchedulers = ImmutableList.builder();

        PlanFragmentId fragmentId = plan.getFragment().getId();
        StageId stageId = new StageId(session.getQueryId(), fragmentId.getId());
        SqlStageExecution stageExecution = createSqlStageExecution(
                new StageExecutionId(stageId, attemptId),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                executor,
                failureDetector,
                schedulerStats,
                tableWriteInfo);

        /**
         * a) TableScan
         * SubPlan使用的{@link PartitioningHandle}参考{@link BasePlanFragmenter#visitTableScan}。
         *
         * b) 有上游依赖的SubPlan
         * SubPlan使用的{@link PartitioningHandle}参考{@link BasePlanFragmenter#visitExchange}。
         *
         * 通过{@link BasePlanFragmenter#createRemoteStreamingExchange}知道，下游SubPlan使用的{@link PartitioningHandle}
         * 和上游的SubPlan使用的{@link PartitioningScheme#partitioning}是保持一致的（其中PartitioningScheme的创建，可以参考
         * {@link com.facebook.presto.sql.planner.optimizations.AddExchanges.Rewriter#visitAggregation}）。
         */
        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        List<RemoteSourceNode> remoteSourceNodes = plan.getFragment().getRemoteSourceNodes();

        /**
         * bucketToPartition用于告知上游stage如何将输出结果对应到不通的partition中，这个会设置到上游stage中的
         * {@link PartitioningScheme#bucketToPartition}。而下游stage，通常会为每个partition创建一个对应的
         * 消费task。
         *
         * 另外，参见{@link StageLinkage#StageLinkage}关于上游stage创建{@link OutputBufferManager}。
         */
        Optional<int[]> bucketToPartition = getBucketToPartition(partitioningHandle, partitioningCache, plan.getFragment().getRoot(), remoteSourceNodes);

        // create child stages
        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StreamingSubPlan stagePlan : plan.getChildren()) {
            List<StageExecutionAndScheduler> subTree = createStreamingLinkedStageExecutions(
                    session,
                    stageExecution::addExchangeLocations,
                    stagePlan.withBucketToPartition(bucketToPartition),
                    partitioningCache,
                    tableWriteInfo,
                    Optional.of(stageExecution),
                    summarizeTaskInfo,
                    remoteTaskFactory,
                    splitSourceFactory,
                    attemptId);
            stageExecutionAndSchedulers.addAll(subTree);
            childStagesBuilder.add(getLast(subTree).getStageExecution());
        }
        Set<SqlStageExecution> childStageExecutions = childStagesBuilder.build();
        stageExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                childStageExecutions.forEach(SqlStageExecution::cancel);
            }
        });

        StageLinkage stageLinkage = new StageLinkage(fragmentId, parent, childStageExecutions);
        StageScheduler stageScheduler = createStageScheduler(
                splitSourceFactory,
                session,
                plan,
                partitioningCache,
                parentStageExecution,
                stageId,
                stageExecution,
                partitioningHandle,
                tableWriteInfo,
                childStageExecutions);
        stageExecutionAndSchedulers.add(new StageExecutionAndScheduler(
                stageExecution,
                stageLinkage,
                stageScheduler));

        return stageExecutionAndSchedulers.build();
    }

    private StageScheduler createStageScheduler(
            SplitSourceFactory splitSourceFactory,
            Session session,
            StreamingSubPlan plan,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            Optional<SqlStageExecution> parentStageExecution,
            StageId stageId,
            SqlStageExecution stageExecution,
            PartitioningHandle partitioningHandle,
            TableWriteInfo tableWriteInfo,
            Set<SqlStageExecution> childStageExecutions)
    {
        Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(plan.getFragment(), session, tableWriteInfo);
        int maxTasksPerStage = getMaxTasksPerStage(session);
        Optional<Predicate<Node>> nodePredicate = getNodePoolSelectionPredicate(plan);
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Map.Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            ConnectorId connectorId = splitSource.getConnectorId();
            if (isInternalSystemConnector(connectorId)) {
                connectorId = null;
            }
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, connectorId, maxTasksPerStage, nodePredicate);
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

            checkArgument(!plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution());
            return newSourcePartitionedSchedulerAsStageScheduler(stageExecution, planNodeId, splitSource, placementPolicy, splitBatchSize);
        }
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStageExecutions.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stageExecution.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Optional<Integer> taskNumberIfScaledWriter = PlanFragmenterUtils.getTableWriterTasks(plan.getFragment().getRoot());

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stageExecution,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(session, null, nodePredicate),
                    scheduledExecutor,
                    getWriterMinSize(session),
                    isOptimizedScaleWriterProducerBuffer(session),
                    taskNumberIfScaledWriter);
            whenAllStages(childStageExecutions, StageExecutionState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            return scheduler;
        }
        else {
            if (!splitSources.isEmpty()) {
                // contains local source
                List<PlanNodeId> schedulingOrder = plan.getFragment().getTableScanSchedulingOrder();
                /**
                 * 通过{@link SystemPartitioningHandle#createSystemPartitioning}可以知道，SystemPartitioningHandle的connectorId
                 * 是为空的，即走到这里要求partitioningHandle不能为SystemPartitioningHandle。换句话说，source connector一定自定义了
                 * {@link ConnectorTablePartitioning}，否则根据{@link BasePlanFragmenter#visitTableScan}中的逻辑可以知道，当connector
                 * 没有自定义ConnectorTablePartitioning，此时SubPlan的PartitioningHandle应该为SOURCE_DISTRIBUTION，则逻辑应该走到
                 * 第一个if分支。
                 */
                ConnectorId connectorId = partitioningHandle.getConnectorId().orElseThrow(IllegalStateException::new);
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                boolean groupedExecutionForStage = plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution();
                if (groupedExecutionForStage) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }

                /**
                 * 由{@link NodePartitioningManager#getBucketNodeMap}和{@link NodePartitioningManager#getNodePartitioningMap}
                 * 知道，当partitioningHandle不为SystemPartitioningHandle时，{@link BucketNodeMap#splitToBucket}总是有意义的。
                 */
                BucketNodeMap bucketNodeMap;
                List<InternalNode> stageNodeList;
                if (plan.getFragment().getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    // no non-replicated remote source
                    boolean dynamicLifespanSchedule = plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule();

                    /**
                     * REPLICATE下，上游stage不用关心bucketToPartition（因为所有partition对应的output buffer内容一样）以及本stage不
                     * 用关心partitionToNode（即本stage任务调度到的node 消费 上游stage的哪个output buffer）。这里会确定bucket的数量和
                     * splitToBucket，以及可能确定bucketToNode（即确定每个split调度到那个node上），具体取决于dynamicLifespanSchedule
                     * 是否为true以及使用的NodeSelectionStrategy。
                     *
                     * 另外，针对{@link BucketNodeMap#isDynamic()}为true的情况，split并不需要绑定到特定的node上执行。失败重试时，它还可
                     * 以迁移到其他其他node上进行执行。
                     */
                    /**
                     * 当GROUPED EXECUTION开启时，dynamicLifespanSchedule将为true，此时返回的BucketNodeMap可能没有bucketToNode
                     * 信息（即NodeSelectionStrategy采用NO_PREFERENCE）。后面调度split时，将由DynamicLifespanScheduler为bucket
                     * 分配对应的node。
                     */
                    bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);

                    /**
                     * 这里有个疑问：要采用dynamic {@link BucketNodeMap}，除了限制和上游stage的shuffle方式为REPLICATE以及connector
                     * 使用的NodeSelectionStrategy为NO_PREFERENCE外，为何还有限定开启GROUPED EXECUTION？不开启GROUPED EXECUTION不
                     * 能使用dynamic {@link BucketNodeMap}？
                     *
                     * 个人理解，限定GROUPED EXECUTION开启是不必要的。但GROUPED EXECUTION不开启的情况下，使用dynamic BucketNodeMap没
                     * 有太大意义，因为要调度的split所属的bucket不受框架控制，connector很可能在split调度时一次性创建出所有的split。而对于
                     * GROUPED EXECUTION开启的情况下则比较有用，因为可以将pending group（也即bucket）尽可能调度到最快完成已有group计算
                     * 的节点上。
                     */
                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    if (bucketNodeMap.hasInitialMap()) {
                        stageNodeList = bucketNodeMap.getBucketToNode().get().stream()
                                .distinct()
                                .collect(toImmutableList());
                    }
                    else {
                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(session, connectorId, nodePredicate).selectRandomNodes(maxTasksPerStage));
                    }
                }
                else {
                    /**
                     * 这里的限制可以参考{@link com.facebook.presto.sql.planner.PlanFragmenterUtils#analyzeGroupedExecution}，
                     * 即存在 非REPLICATE 的 {@link RemoteSourceNode}时，StageExecutionStrategy只可能是 UNGROUPED_EXECUTION
                     * 或者 FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION。
                     */
                    // cannot use dynamic lifespan schedule
                    verify(!plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule(),
                            "Stage was planned with DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION, but is not eligible.");

                    /**
                     * 此时bucketToNode以及partitionToNode都需要确定，其实在{@link #createStreamingLinkedStageExecutions}中调用
                     * {@link #getBucketToPartition}就已经确定好了NodePartitionMap。因为需要保证上游stage看到的bucketToPartition
                     * 同本stage使用的bucketToNode以及partitionToNode的一致性，这里使用了cache来确保多次resolve出来的NodePartitionMap
                     * 是相同的。
                     */
                    // Partitioned remote source requires nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                    if (groupedExecutionForStage) {
                        /**
                         * [question] 思考下为啥remote source全是REPLICATE的情况下，没有这个限制，而此处却有？
                         */
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    }
                    stageNodeList = nodePartitionMap.getPartitionToNode();
                    bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                }

                FixedSourcePartitionedScheduler fixedSourcePartitionedScheduler = new FixedSourcePartitionedScheduler(
                        stageExecution,
                        splitSources,
                        plan.getFragment().getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(session, connectorId, nodePredicate),
                        connectorPartitionHandles);
                if (plan.getFragment().getStageExecutionDescriptor().isRecoverableGroupedExecution()) {
                    stageExecution.registerStageTaskRecoveryCallback(taskId -> {
                        checkArgument(taskId.getStageExecutionId().getStageId().equals(stageId), "The task did not execute this stage");
                        checkArgument(parentStageExecution.isPresent(), "Parent stage execution must exist");
                        checkArgument(parentStageExecution.get().getAllTasks().size() == 1, "Parent stage should only have one task for recoverable grouped execution");

                        parentStageExecution.get().removeRemoteSourceIfSingleTaskStage(taskId);
                        fixedSourcePartitionedScheduler.recover(taskId);
                    });
                }
                return fixedSourcePartitionedScheduler;
            }

            else {
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                return new FixedCountScheduler(stageExecution, partitionToNode);
            }
        }
    }

    private Optional<Predicate<Node>> getNodePoolSelectionPredicate(StreamingSubPlan plan)
    {
        if (!isEnableWorkerIsolation || plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution()) {
            //skipping node pool based selection for grouped execution
            return Optional.empty();
        }
        NodePoolType workerPoolType = plan.getFragment().isLeaf() ? LEAF : INTERMEDIATE;
        return Optional.of(node -> node.getPoolType().equals(workerPoolType));
    }

    private static Optional<int[]> getBucketToPartition(
            PartitioningHandle partitioningHandle,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            PlanNode fragmentRoot,
            List<RemoteSourceNode> remoteSourceNodes)
    {
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            return Optional.of(new int[1]);
        }
        else if (PlanNodeSearcher.searchFrom(fragmentRoot).where(node -> node instanceof TableScanNode).findFirst().isPresent()) {
            if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                return Optional.empty();
            }
            else {
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }
        else {
            NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
            List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
            // todo this should asynchronously wait a standard timeout period before failing
            checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
            return Optional.of(nodePartitionMap.getBucketToPartition());
        }
    }

    private static ListenableFuture<?> whenAllStages(Collection<SqlStageExecution> stageExecutions, Predicate<StageExecutionState> predicate)
    {
        checkArgument(!stageExecutions.isEmpty(), "stageExecutions is empty");
        Set<StageExecutionId> stageIds = newConcurrentHashSet(stageExecutions.stream()
                .map(SqlStageExecution::getStageExecutionId)
                .collect(toSet()));
        SettableFuture<?> future = SettableFuture.create();

        for (SqlStageExecution stage : stageExecutions) {
            stage.addStateChangeListener(state -> {
                if (predicate.test(state) && stageIds.remove(stage.getStageExecutionId()) && stageIds.isEmpty()) {
                    future.set(null);
                }
            });
        }

        return future;
    }
}
