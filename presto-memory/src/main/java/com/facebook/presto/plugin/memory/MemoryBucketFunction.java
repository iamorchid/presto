package com.facebook.presto.plugin.memory;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;

import java.util.List;

import static com.facebook.presto.common.type.TypeUtils.hashPosition;
import static com.google.common.base.Preconditions.checkArgument;

public class MemoryBucketFunction implements BucketFunction
{
    private final int bucketCount;
    private final List<Type> types;

    public MemoryBucketFunction(int bucketCount, List<Type> types) {
        this.bucketCount = bucketCount;
        this.types = types;
    }

    @Override
    public int getBucket(Page page, int position) {
        checkArgument(types.size() == page.getChannelCount());
        return (getHashCode(page, position) & Integer.MAX_VALUE) % bucketCount;
    }

    private int getHashCode(Page page, int position)
    {
        int result = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            int fieldHash = (int) hashPosition(types.get(i), page.getBlock(i), position);
            result = result * 31 + fieldHash;
        }
        return result;
    }
}
