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

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TpchPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final String partitioningTable;
    private final long totalRows;
    private final boolean groupedExecutionDisabled;

    @JsonCreator
    public TpchPartitioningHandle(@JsonProperty("partitioningTable") String partitioningTable,
                                  @JsonProperty("totalRows") long totalRows,
                                  @JsonProperty("groupedExecutionDisabled") boolean groupedExecutionDisabled)
    {
        this.partitioningTable = requireNonNull(partitioningTable, "table is null");

        checkArgument(totalRows > 0, "totalRows must be at least 1");
        this.totalRows = totalRows;

        this.groupedExecutionDisabled = groupedExecutionDisabled;
    }

    @JsonProperty
    public String getPartitioningTable()
    {
        return partitioningTable;
    }

    @JsonProperty
    public long getTotalRows()
    {
        return totalRows;
    }

    @JsonProperty
    public boolean isGroupedExecutionDisabled()
    {
        return groupedExecutionDisabled;
    }

    /**
     * 在判断两张表进行join时能否采用colocated-join，会check两种表的{@link ConnectorPartitioningHandle}是否一致，用到
     * 的方法就是{@link #equals(Object)}，因此这里判断equals不包含{@link #groupedExecutionDisabled}。
     *
     * 具体参见{@link com.facebook.presto.sql.planner.optimizations.AddExchanges.Rewriter#planPartitionedJoin}。
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TpchPartitioningHandle that = (TpchPartitioningHandle) o;
        return Objects.equals(partitioningTable, that.partitioningTable) &&
                totalRows == that.totalRows;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningTable, totalRows);
    }

    @Override
    public String toString()
    {
        return partitioningTable + ":" + totalRows;
    }
}
