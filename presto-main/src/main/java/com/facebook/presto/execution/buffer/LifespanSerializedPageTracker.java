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
import com.facebook.presto.execution.buffer.SerializedPageReference.PagesReleasedListener;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class LifespanSerializedPageTracker
        implements PagesReleasedListener
{
    private final OutputBufferMemoryManager memoryManager;
    @Nullable
    private final PagesReleasedListener childListener;
    private final ConcurrentMap<Lifespan, AtomicLong> outstandingPageCountPerLifespan = new ConcurrentHashMap<>();
    private final Set<Lifespan> noMorePagesForLifespan = ConcurrentHashMap.newKeySet();
    private volatile Consumer<Lifespan> lifespanCompletionCallback;

    public LifespanSerializedPageTracker(OutputBufferMemoryManager memoryManager)
    {
        this(memoryManager, Optional.empty());
    }

    public LifespanSerializedPageTracker(OutputBufferMemoryManager memoryManager, Optional<PagesReleasedListener> childListener)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.childListener = requireNonNull(childListener, "childListener is null").orElse(null);
    }

    public boolean isLifespanCompletionCallbackRegistered()
    {
        return lifespanCompletionCallback != null;
    }

    public void registerLifespanCompletionCallback(Consumer<Lifespan> callback)
    {
        checkState(lifespanCompletionCallback == null, "lifespanCompletionCallback is already set");
        this.lifespanCompletionCallback = requireNonNull(callback, "callback is null");
    }

    public void incrementLifespanPageCount(Lifespan lifespan, int pagesAdded)
    {
        // JDK-8 acquires the write lock unconditionally in computeIfAbsent
        // TODO: Remove this extra get call once Presto no longer supports JDK-8
        AtomicLong counter = outstandingPageCountPerLifespan.get(lifespan);
        if (counter == null) {
            counter = outstandingPageCountPerLifespan.computeIfAbsent(lifespan, ignore -> new AtomicLong());
        }
        counter.addAndGet(pagesAdded);
    }

    public void setNoMorePagesForLifespan(Lifespan lifespan)
    {
        requireNonNull(lifespan, "lifespan is null");
        noMorePagesForLifespan.add(lifespan);
    }

    public boolean isNoMorePagesForLifespan(Lifespan lifespan)
    {
        return noMorePagesForLifespan.contains(lifespan);
    }

    public boolean isFinishedForLifespan(Lifespan lifespan)
    {
        if (!noMorePagesForLifespan.contains(lifespan)) {
            return false;
        }

        AtomicLong outstandingPageCount = outstandingPageCountPerLifespan.get(lifespan);
        return outstandingPageCount == null || outstandingPageCount.get() == 0;
    }

    /**
     * 执行到下面方法的主题逻辑大致为：
     * 1）消费端ack已经消费的数据，{@link PartitionedOutputBuffer#acknowledge}
     * 2）减少buffer中page的引用计数，{@link SerializedPageReference#dereferencePages}
     * 3）执行下面的onPagesReleased
     * 4）执行lifespanCompletionCallback，对应于{@link com.facebook.presto.execution.SqlTaskExecution.Status#checkLifespanCompletion}
     * 5）更新output buffer内存使用统计，以便unblock之前因为buffer满而导致的output阻塞的driver
     */
    @Override
    public void onPagesReleased(Lifespan lifespan, int releasedPageCount, long releasedSizeInBytes)
    {
        long outstandingPageCount = outstandingPageCountPerLifespan.get(lifespan).addAndGet(-releasedPageCount);
        if (outstandingPageCount == 0 && noMorePagesForLifespan.contains(lifespan)) {
            Consumer<Lifespan> lifespanCompletionCallback = this.lifespanCompletionCallback;
            checkState(lifespanCompletionCallback != null, "lifespanCompletionCallback is not null");
            lifespanCompletionCallback.accept(lifespan);
        }
        memoryManager.updateMemoryUsage(-releasedSizeInBytes);
        if (childListener != null) {
            childListener.onPagesReleased(lifespan, releasedPageCount, releasedSizeInBytes);
        }
    }
}
