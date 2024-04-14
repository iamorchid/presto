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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MemoryPagesStore pagesStore;
    private final HostAddress currentHostAddress;

    @Inject
    public MemoryPageSinkProvider(MemoryPagesStore pagesStore, NodeManager nodeManager)
    {
        this(pagesStore, requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getHostAndPort());
    }

    @VisibleForTesting
    public MemoryPageSinkProvider(MemoryPagesStore pagesStore, HostAddress currentHostAddress)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkContext pageSinkContext)
    {
        checkArgument(!pageSinkContext.isCommitRequired(), "Memory connector does not support page sink commit");

        MemoryOutputTableHandle memoryOutputTableHandle = (MemoryOutputTableHandle) outputTableHandle;
        MemoryTableHandle tableHandle = memoryOutputTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(memoryOutputTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(memoryOutputTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId);
        return createPageSink(pagesStore, currentHostAddress, tableId, tableHandle.getColumnHandles(), tableHandle.getBucketProperty());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkContext pageSinkContext)
    {
        checkArgument(!pageSinkContext.isCommitRequired(), "Memory connector does not support page sink commit");

        MemoryInsertTableHandle memoryInsertTableHandle = (MemoryInsertTableHandle) insertTableHandle;
        MemoryTableHandle tableHandle = memoryInsertTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(memoryInsertTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(memoryInsertTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId);
        return createPageSink(pagesStore, currentHostAddress, tableId, tableHandle.getColumnHandles(), tableHandle.getBucketProperty());
    }

    private static MemoryPageSink createPageSink(MemoryPagesStore pagesStore, HostAddress currentHostAddress, long tableId,
                                                 List<MemoryColumnHandle> columnHandles, Optional<MemoryBucketProperty> bucketProperty)
    {
        if (bucketProperty.isPresent()) {
            return new MemoryBucketedPageSink(pagesStore, currentHostAddress, tableId, columnHandles, bucketProperty.get());
        }
        return new MemoryPageSink(pagesStore, currentHostAddress, tableId);
    }

    private static class MemoryPageSink
            implements ConnectorPageSink
    {
        protected final MemoryPagesStore pagesStore;
        protected final HostAddress currentHostAddress;
        protected final long tableId;
        private long addedRows;

        public MemoryPageSink(MemoryPagesStore pagesStore, HostAddress currentHostAddress, long tableId)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
            this.tableId = tableId;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            pagesStore.add(tableId, page);
            addedRows += page.getPositionCount();
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            MemoryDataFragment frag = new MemoryDataFragment(currentHostAddress, 0, addedRows);
            return completedFuture(ImmutableList.of(frag.toSlice()));
        }

        @Override
        public void abort()
        {
        }
    }

    private static class MemoryBucketedPageSink
        extends MemoryPageSink
    {
        private final Type[] inputColumnTypes;
        private final PageBuilder[] pageBuilders;
        private final long[] bucketAddedRows;

        private final BucketFunction bucketFunction;
        private final int[] bucketColumns;

        public MemoryBucketedPageSink(MemoryPagesStore pagesStore, HostAddress currentHostAddress, long tableId,
                              List<MemoryColumnHandle> inputColumns, MemoryBucketProperty bucketProperty)
        {
            super(pagesStore, currentHostAddress, tableId);

            checkState(!bucketProperty.getBucketedBy().isEmpty(), "no bucket columns specified");

            this.inputColumnTypes = new Type[inputColumns.size()];
            for (int index = 0; index < inputColumns.size(); index++) {
                inputColumnTypes[index] = inputColumns.get(index).getColumnType();
            }

            this.pageBuilders = new PageBuilder[bucketProperty.getBuckets()];
            for (int i = 0; i < pageBuilders.length; i++) {
                pageBuilders[i] = PageBuilder.withMaxPageSize(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, Arrays.asList(inputColumnTypes));
            }

            this.bucketAddedRows = new long[bucketProperty.getBuckets()];

            Map<String, Type> columnNameToTypeMap = inputColumns.stream()
                    .collect(Collectors.toMap(MemoryColumnHandle::getName, MemoryColumnHandle::getColumnType));
            List<Type> bucketColumnTypes = bucketProperty.getBucketedBy().stream()
                    .map(columnNameToTypeMap::get)
                    .collect(Collectors.toList());
            this.bucketFunction = new MemoryBucketFunction(bucketProperty.getBuckets(), bucketColumnTypes);

            Map<String, Integer> columnNameToIndexMap = new HashMap<>();
            for (int index = 0; index < inputColumns.size(); index++) {
                columnNameToIndexMap.put(inputColumns.get(index).getName(), index);
            }
            this.bucketColumns = bucketProperty.getBucketedBy().stream()
                    .mapToInt(columnNameToIndexMap::get)
                    .toArray();
        }

        private void bucketPage(Page page)
        {
            Page bucketColumnsPage = page.extractChannels(bucketColumns);
            for (int position = 0; position < page.getPositionCount(); position += 1) {
                int bucket = bucketFunction.getBucket(bucketColumnsPage, position);
                appendRow(pageBuilders[bucket], page, position);
            }
            flush(false);
        }

        private void appendRow(PageBuilder pageBuilder, Page page, int position)
        {
            pageBuilder.declarePosition();

            for (int channel = 0; channel < inputColumnTypes.length; channel++) {
                Type type = inputColumnTypes[channel];
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        private void flush(boolean force)
        {
            for (int bucket = 0; bucket < pageBuilders.length; bucket++) {
                PageBuilder pageBuilder = pageBuilders[bucket];
                if (!pageBuilder.isEmpty() && (force || pageBuilder.isFull())) {
                    Page page = pageBuilder.build();
                    pageBuilder.reset();

                    pagesStore.add(tableId, bucket, page);
                    bucketAddedRows[bucket] += page.getPositionCount();
                }
            }
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            bucketPage(page);
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            flush(true);

            ImmutableList.Builder<Slice> frags = ImmutableList.builder();
            for (int bucket = 0; bucket < bucketAddedRows.length; bucket++) {
                if (bucketAddedRows[bucket] > 0) {
                    MemoryDataFragment frag = new MemoryDataFragment(currentHostAddress, bucket, bucketAddedRows[bucket]);
                    frags.add(frag.toSlice());
                }
            }
            return completedFuture(frags.build());
        }

        @Override
        public void abort()
        {
        }
    }
}
