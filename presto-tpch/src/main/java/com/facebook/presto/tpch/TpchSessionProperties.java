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

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

public class TpchSessionProperties {
    /**
     * 参考{@link com.facebook.presto.spi.ConnectorTableLayout#getTablePartitioning()}
     */
    public static final String DISABLE_ORDERS_PARTITIONING = "disable_orders_partitioning";
    public static final String DISABLE_LINEITEM_PARTITIONING = "disable_lineitem_partitioning";

    /**
     * 参考{@link com.facebook.presto.spi.ConnectorTableLayout#getStreamPartitioningColumns()}
     */
    public static final String DISABLE_STREAM_PARTITIONING = "disable_stream_partitioning";

    /**
     * 参考{@link com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider#listPartitionHandles}
     */
    public static final String DISABLE_ORDERS_GROUPED_EXECUTION = "disable_orders_grouped_execution";

    public static final String FAILED_PARTITIONS = "failed_partitions";

    private final List<PropertyMetadata<?>> sessionProperties;

    public TpchSessionProperties()
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        DISABLE_ORDERS_PARTITIONING,
                        "Disable partitioning for orders table",
                        false,
                        false),
                booleanProperty(
                        DISABLE_LINEITEM_PARTITIONING,
                        "Disable partitioning for lineitem table",
                        false,
                        false),
                booleanProperty(
                        DISABLE_STREAM_PARTITIONING,
                        "Disable stream partitioning",
                        false,
                        false),
                booleanProperty(
                        DISABLE_ORDERS_GROUPED_EXECUTION,
                        "Disable grouped execution for orders table",
                        false,
                        false),
                stringProperty(
                        FAILED_PARTITIONS,
                        "Partitions we want to simulate failure for",
                        "",
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
