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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.BucketFunction;
import io.airlift.tpch.*;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.toIntExact;

public class TpchBucketFunction
        implements BucketFunction
{
    private final int bucketCount;
    private final long rowsPerBucket;

    public TpchBucketFunction(int bucketCount, long rowsPerBucket)
    {
        this.bucketCount = bucketCount;
        this.rowsPerBucket = rowsPerBucket;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        Block block = page.getBlock(0);
        if (block.isNull(position)) {
            return 0;
        }

        long orderKey = BIGINT.getLong(block, position);
        long rowNumber = rowNumberFromOrderKey(orderKey);

        /**
         * 这里的{@link #rowsPerBucket}针对的是{@link TpchTable#ORDERS}。对于{@link TpchTable#LINE_ITEM}
         * 而言，因为一个order会对应多个lineitem，因此{@link TpchTable#LINE_ITEM}的bucket中包含的rows肯定是
         * 多余这里的{@link #rowsPerBucket}的。
         */
        int bucket = toIntExact(rowNumber / rowsPerBucket);

        // due to rounding, the last bucket has extra rows
        if (bucket >= bucketCount) {
            bucket = bucketCount - 1;
        }
        return bucket;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketCount", bucketCount)
                .add("rowsPerBucket", rowsPerBucket)
                .toString();
    }

    /**
     * {@link OrderGenerator#makeOrderKey}的逆运算，计算对应order的row number（它是从1开始, 计算bucket时,
     * 需要转成从0开始的序号）。基于order的行号，可以快速计算出order或者lineitem所属的bucket。
     */
    private static long rowNumberFromOrderKey(long orderKey)
    {
        // remove bits 3 and 4
        return (((orderKey & ~(0b11_111)) >>> 2) | orderKey & 0b111) - 1;
    }
}
