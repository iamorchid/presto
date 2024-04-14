package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class MemoryPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final MemoryBucketProperty bucketProperty;

    @JsonCreator
    public MemoryPartitioningHandle(@JsonProperty("bucketProperty") MemoryBucketProperty bucketProperty) {
        this.bucketProperty = bucketProperty;
    }

    @JsonProperty
    public MemoryBucketProperty getBucketProperty() {
        return bucketProperty;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryPartitioningHandle that = (MemoryPartitioningHandle) o;
        return Objects.equals(bucketProperty, that.bucketProperty);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketProperty);
    }
}
