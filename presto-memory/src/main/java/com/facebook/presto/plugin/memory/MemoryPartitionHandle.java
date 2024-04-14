package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class MemoryPartitionHandle extends ConnectorPartitionHandle {
    private final int bucket;

    @JsonCreator
    public MemoryPartitionHandle(@JsonProperty("bucket") int bucket)
    {
        this.bucket = bucket;
    }

    @JsonProperty
    public int getBucket()
    {
        return bucket;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MemoryPartitionHandle that = (MemoryPartitionHandle) o;
        return bucket == that.bucket;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucket);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucket", bucket)
                .toString();
    }
}
