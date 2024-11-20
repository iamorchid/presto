package com.facebook.presto.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class MemoryBucketProperty {
    private final int buckets;
    private final List<String> bucketedBy;

    public static Optional<MemoryBucketProperty> create(Map<String, Object> tableProperties) {
        List<String> bucketedBy = Splitter.on(",")
                .trimResults()
                .omitEmptyStrings()
                .splitToList((String)tableProperties.get(MemoryTableProperties.BUCKETED_BY));
        if (bucketedBy.isEmpty()) {
            return Optional.empty();
        }

        int buckets = (int)tableProperties.get(MemoryTableProperties.BUCKETS);
        checkArgument(buckets > 0, "invalid buckets property");

        return Optional.of(new MemoryBucketProperty(buckets, bucketedBy));
    }

    @JsonCreator
    public MemoryBucketProperty(@JsonProperty("buckets") int buckets,
                                @JsonProperty("bucketedBy") List<String> bucketedBy) {
        this.buckets = buckets;
        this.bucketedBy = bucketedBy;
    }

    @JsonProperty
    public int getBuckets() {
        return buckets;
    }

    @JsonProperty
    public List<String> getBucketedBy() {
        return bucketedBy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryBucketProperty that = (MemoryBucketProperty) o;
        return buckets == that.buckets && Objects.equals(bucketedBy, that.bucketedBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buckets, bucketedBy);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("buckets", buckets)
                .add("bucketedBy", bucketedBy)
                .toString();
    }
}
