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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_PAGES;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.SerializedPageReference.dereferencePages;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputBuffer
        implements OutputBuffer
{
    private final StateMachine<BufferState> state;
    private final OutputBuffers outputBuffers;
    private final OutputBufferMemoryManager memoryManager;
    private final LifespanSerializedPageTracker pageTracker;

    private final List<ClientBuffer> partitions;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public PartitionedOutputBuffer(
            String taskInstanceId,
            StateMachine<BufferState> state,
            OutputBuffers outputBuffers,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor)
    {
        this.state = requireNonNull(state, "state is null");

        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers.getType() == PARTITIONED, "Expected a PARTITIONED output buffer descriptor");
        checkArgument(outputBuffers.isNoMoreBufferIds(), "Expected a final output buffer descriptor");
        this.outputBuffers = outputBuffers;

        this.memoryManager = new OutputBufferMemoryManager(
                requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes(),
                requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
        this.pageTracker = new LifespanSerializedPageTracker(memoryManager);

        ImmutableList.Builder<ClientBuffer> partitions = ImmutableList.builder();
        for (OutputBufferId bufferId : outputBuffers.getBuffers().keySet()) {
            ClientBuffer partition = new ClientBuffer(taskInstanceId, bufferId, pageTracker);
            partitions.add(partition);
        }
        this.partitions = partitions.build();

        state.compareAndSet(OPEN, NO_MORE_BUFFERS);
        state.compareAndSet(NO_MORE_PAGES, FLUSHING);
        checkFlushComplete();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    @Override
    public boolean isOverutilized()
    {
        return memoryManager.isOverutilized();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = this.state.get();

        int totalBufferedPages = 0;
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builderWithExpectedSize(partitions.size());
        for (ClientBuffer partition : partitions) {
            BufferInfo bufferInfo = partition.getInfo();
            infos.add(bufferInfo);

            totalBufferedPages += bufferInfo.getPageBufferInfo().getBufferedPages();
        }

        return new OutputBufferInfo(
                "PARTITIONED",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages,
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                infos.build());
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    @Override
    public void registerLifespanCompletionCallback(Consumer<Lifespan> callback)
    {
        pageTracker.registerLifespanCompletionCallback(callback);
    }

    @Override
    public void enqueue(Lifespan lifespan, List<SerializedPage> pages)
    {
        checkState(partitions.size() == 1, "Expected exactly one partition");
        enqueue(lifespan, 0, pages);
    }

    @Override
    public void enqueue(Lifespan lifespan, int partitionNumber, List<SerializedPage> pages)
    {
        requireNonNull(lifespan, "lifespan is null");
        requireNonNull(pages, "pages is null");
        checkState(pageTracker.isLifespanCompletionCallbackRegistered(), "lifespanCompletionCallback must be set before enqueueing data");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages() || pageTracker.isNoMorePagesForLifespan(lifespan)) {
            return;
        }

        ImmutableList.Builder<SerializedPageReference> references = ImmutableList.builderWithExpectedSize(pages.size());
        long bytesAdded = 0;
        long rowCount = 0;
        for (SerializedPage page : pages) {
            long retainedSize = page.getRetainedSizeInBytes();
            bytesAdded += retainedSize;
            rowCount += page.getPositionCount();
            // create page reference counts with an initial single reference
            references.add(new SerializedPageReference(page, 1, lifespan));
        }
        List<SerializedPageReference> serializedPageReferences = references.build();

        /**
         * 如果执行到这里时，已经发生了{@link #fail()}，则下面的updateMemoryUsage操作将没有任何效果，因为memoryManager
         * 已经close了。换句话说，这里新加入的pages虽然占用了heap空间，但它们不会统计到memory context中。
         */
        // reserve memory
        memoryManager.updateMemoryUsage(bytesAdded);

        // update stats
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(serializedPageReferences.size());
        pageTracker.incrementLifespanPageCount(lifespan, serializedPageReferences.size());

        // add pages to the buffer (this will increase the reference count by one)
        partitions.get(partitionNumber).enqueuePages(serializedPageReferences);

        // drop the initial reference
        dereferencePages(serializedPageReferences, pageTracker);
    }

    @Override
    public void setNoMorePagesForLifespan(Lifespan lifespan)
    {
        pageTracker.setNoMorePagesForLifespan(lifespan);
    }

    @Override
    public boolean isFinishedForLifespan(Lifespan lifespan)
    {
        return pageTracker.isFinishedForLifespan(lifespan);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return partitions.get(outputBufferId.getId()).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId outputBufferId, long sequenceId)
    {
        requireNonNull(outputBufferId, "bufferId is null");

        partitions.get(outputBufferId.getId()).acknowledgePages(sequenceId);
    }

    /**
     * 当{@link #fail()}执行后，bufferId对应的consumer因为crash而没有调用这个方法，那么bufferId
     * 对应的{@link ClientBuffer}还能得到清理吗？
     */
    @Override
    public void abort(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        partitions.get(bufferId.getId()).destroy();

        checkFlushComplete();
    }

    /**
     * 关于setNoMorePages()的调用时机，参见：
     * {@link com.facebook.presto.execution.SqlTaskExecution#checkTaskCompletion()}
     */
    @Override
    public void setNoMorePages()
    {
        state.compareAndSet(OPEN, NO_MORE_PAGES);
        state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
        memoryManager.setNoBlockOnFull();

        partitions.forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            /**
             * 调用{@link ClientBuffer#destroy()}后，ClientBuffer将清理内存中的pages，同时不再接受新的pages。
             * 后续对ClientBuffer的读取，将直接返回bufferComplete为true的空结构，即告诉消费端没有更多数据了。
             */
            partitions.forEach(ClientBuffer::destroy);

            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
        }
    }

    @Override
    public void fail()
    {
        /**
         * {@link com.facebook.presto.execution.TaskState}的终态分为4种情况:
         * a) FINISHED => 任务成功完成且output被完全消费
         * b) CANCELED => 任务被用户提前终止且已有的output被完全消费（发生在stage被cancel）
         * c) ABORTED  => query中其他任务FAILED 或者 query被整个cancel
         * d) FAILED   => 当前任务执行失败
         *
         * 任务状态FINISHED和CANCELED 将对应于 {@link BufferState#FINISHED}，而ABORTED和FAILED
         * 则对应于 {@link BufferState#FAILED}。
         *
         * 参考：
         * {@link com.facebook.presto.execution.SqlQueryExecution#cancelStage}
         * {@link com.facebook.presto.execution.SqlQueryExecution#cancelQuery}
         */
        // ignore fail if the buffer already in a terminal state.
        if (state.setIf(FAILED, oldState -> !oldState.isTerminal())) {
            memoryManager.setNoBlockOnFull();

            /**
             * 调用这个操作后，可以将之前output buffer占用的内存归还给底层MemoryPool（但这个仅仅是数字上的统计），但pages真正
             * 占用的内存此时并不能回收（{@link ClientBuffer#destroy()}尚未执行，需要等到消费端进行abortResults操作）。因次，
             * MemoryPool此时看到的可用内存是水份的。
             *
             * 参考:
             * {@link com.facebook.presto.server.TaskResource#abortResults}
             */
            forceFreeMemory();

            // DO NOT destroy buffers or set no more pages. The coordinator manages the teardown of failed queries.
            /**
             * 如果coordinator此时crash并重启的话，{@link #abort(OutputBufferId)}得不到执行，那么{@link #partitions}对应
             * 的ClientBuffer还能被清理吗？
             *
             * 答案是：能，虽然{@link ClientBuffer#destroy()}不会再执行，但GC会帮我们回收掉{@link ClientBuffer#pages}，因为
             * {@link SqlTaskManager#failAbandonedTasks()}和{@link SqlTaskManager#removeOldTasks()}会保证清除掉残留的
             * SqlTask，并最终GC掉和它相关的其他资源。
             *
             * 尽管调用{@link ClientBuffer#destroy()}可以更及时地释放pages，但目前的实现会导致消费端（即从当前task的output读取
             * 数据的下游）误以为已经成功消费了所有的数据（因为{@link ClientBuffer#destroy()}会设置noMorePages）。因此，这里需要
             * 避免下游任务非预期的成功退出，即block住下游任务的消费，让它等待coordinator进行abort操作。
             */
        }
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return memoryManager.getPeakMemoryUsage();
    }

    @VisibleForTesting
    void forceFreeMemory()
    {
        memoryManager.close();
    }

    private void checkFlushComplete()
    {
        if (state.get() != FLUSHING && state.get() != NO_MORE_BUFFERS) {
            return;
        }

        if (partitions.stream().allMatch(ClientBuffer::isDestroyed)) {
            destroy();
        }
    }

    @VisibleForTesting
    OutputBufferMemoryManager getMemoryManager()
    {
        return memoryManager;
    }
}
