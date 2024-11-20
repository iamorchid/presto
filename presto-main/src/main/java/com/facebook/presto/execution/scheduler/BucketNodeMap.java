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

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

public abstract class BucketNodeMap
{
    private final ToIntFunction<Split> splitToBucket;

    public BucketNodeMap(ToIntFunction<Split> splitToBucket)
    {
        this.splitToBucket = requireNonNull(splitToBucket, "splitToBucket is null");
    }

    public abstract int getBucketCount();

    public abstract Optional<InternalNode> getAssignedNode(int bucketedId);

    public abstract boolean isBucketCacheable(int bucketedId);

    public abstract void assignOrUpdateBucketToNode(int bucketedId, InternalNode node, boolean cacheable);

    /**
     * {@link #isDynamic()}为true，表示bucket和node没有定义固定的映射关系，调度层可以根据需要动态设置bucket
     * 和node映射关系。而采用dynamic的映射关系是有条件的，即如果存在上游stage，则上游必须采用broadcast方式来输
     * 出数据到下游stage（即使用当前{@link BucketNodeMap}调度split的stage）。
     */
    public abstract boolean isDynamic();

    public abstract boolean hasInitialMap();

    public final Optional<InternalNode> getAssignedNode(Split split)
    {
        return getAssignedNode(splitToBucket.applyAsInt(split));
    }

    public final boolean isSplitCacheable(Split split)
    {
        return isBucketCacheable(splitToBucket.applyAsInt(split));
    }

    public abstract Optional<List<InternalNode>> getBucketToNode();
}
