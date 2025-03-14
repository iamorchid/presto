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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;

import static java.util.concurrent.CompletableFuture.completedFuture;

public final class MemorySplitManager
        implements ConnectorSplitManager
{
    private final int splitsPerNode;
    private final int splitsPerBucket;

    @Inject
    public MemorySplitManager(MemoryConfig config)
    {
        this.splitsPerNode = config.getSplitsPerNode();
        this.splitsPerBucket = config.getSplitsPerBucket();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layoutHandle,
            SplitSchedulingContext splitSchedulingContext)
    {
        final MemoryTableLayoutHandle layout = (MemoryTableLayoutHandle) layoutHandle;
        final List<MemoryDataFragment> dataFragments = layout.getDataFragments();
        final int splitsPerFrag = layout.getTable().getBucketProperty().isPresent() ? splitsPerBucket : splitsPerNode;

        Function<List<MemoryDataFragment>, List<ConnectorSplit>> splitGenerator = frags -> {
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (MemoryDataFragment dataFragment : frags) {
                /**
                 * 每个split会对应一个独立的Driver，这里将一个node上的数据分成多个splits，便于
                 * 并发地处理一个节点上的数据。
                 */
                for (int i = 0; i < splitsPerFrag; i++) {
                    splits.add(
                            new MemorySplit(
                                    layout.getTable(),
                                    dataFragment.getBucket(),
                                    i,
                                    splitsPerFrag,
                                    dataFragment.getHostAddress(),
                                    dataFragment.getRows()));
                }
            }
            return splits.build();
        };

        if (splitSchedulingContext.getSplitSchedulingStrategy() == SplitSchedulingStrategy.UNGROUPED_SCHEDULING) {
            return new FixedSplitSource(splitGenerator.apply(dataFragments));
        }

        return new ConnectorSplitSource() {
            private final List<MemoryDataFragment> remainingFrags = new ArrayList<>(dataFragments);

            @Override
            public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize) {
                final int bucket = ((MemoryPartitionHandle) partitionHandle).getBucket();
                final List<MemoryDataFragment> targetFrags = new ArrayList<>();
                Iterator<MemoryDataFragment> it = remainingFrags.iterator();
                while(it.hasNext()) {
                    MemoryDataFragment frag = it.next();
                    if (frag.getBucket() == bucket) {
                        targetFrags.add(frag);
                        it.remove();
                    }
                }
                // 这里限定了一个bucket的数据不会跨多个机器
                checkState(targetFrags.size() <= 1, "each bucket should only have one data fragment");
                return completedFuture(new ConnectorSplitBatch(splitGenerator.apply(targetFrags), true));
            }

            @Override
            public void close() {

            }

            @Override
            public boolean isFinished() {
                return remainingFrags.isEmpty();
            }
        };
    }
}
