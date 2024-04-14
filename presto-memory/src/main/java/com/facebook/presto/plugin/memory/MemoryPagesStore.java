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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.memory.MemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static com.facebook.presto.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static java.lang.String.format;

@ThreadSafe
public class MemoryPagesStore
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes;

    private final Map<Long, TableData> tables = new HashMap<>();

    @Inject
    public MemoryPagesStore(MemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
    }

    public synchronized void initialize(long tableId)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData());
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        add(tableId, 0, page);
    }

    public synchronized void add(Long tableId, int bucket, Page page)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new PrestoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        TableData tableData = tables.get(tableId);
        tableData.add(bucket, page);
    }

    public synchronized List<Page> getPages(
            Long tableId,
            int bucket,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        BucketedTableData bucketedTableData = tables.get(tableId).getBucketedTableData(bucket);
        if (bucketedTableData.getRows() < expectedRows) {
            throw new PrestoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, bucketedTableData.getRows()));
        }

        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        /**
         * 按照partNumber, partNumber + totalParts, partNumber + 2 * totalParts, ... 方式来对pages进行划分。
         */
        for (int i = partNumber; i < bucketedTableData.getPages().size(); i += totalParts) {
            partitionedPages.add(getColumns(bucketedTableData.getPages().get(i), columnIndexes));
        }

        return partitionedPages.build();
    }

    public synchronized boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when MemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which MemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we can not determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue().getPages()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tableDataIterator.remove();
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = page.getBlock(columnIndexes.get(i));
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    private static final class TableData
    {
        private final Map<Integer, BucketedTableData> bucketedData = new HashMap<>();
        private long totalRows = 0;

        public void add(int bucket, Page page)
        {
            bucketedData.computeIfAbsent(bucket, BucketedTableData::new).add(page);
            totalRows += page.getPositionCount();
        }

        public BucketedTableData getBucketedTableData(int bucket)
        {
            return bucketedData.computeIfAbsent(bucket, BucketedTableData::new);
        }

        public List<Page> getPages()
        {
            return bucketedData.values().stream().flatMap(b -> b.getPages().stream()).collect(Collectors.toList());
        }

        public long getTotalRows()
        {
            return totalRows;
        }
    }

    private static final class BucketedTableData
    {
        private final int bucket;
        private final List<Page> pages = new ArrayList<>();
        private long rows;

        public BucketedTableData(int bucket)
        {
            this.bucket = bucket;
        }

        public void add(Page page)
        {
            pages.add(page);
            rows += page.getPositionCount();
        }

        private int getBucket()
        {
            return bucket;
        }

        private List<Page> getPages()
        {
            return pages;
        }

        private long getRows()
        {
            return rows;
        }
    }
}
