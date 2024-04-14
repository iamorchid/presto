package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.*;

public class MemoryTableProperties {
    public final static String BUCKETS = "buckets";
    public final static String BUCKETED_BY = "bucketed-by";

    public final static MemoryTableProperties INSTANCE = new MemoryTableProperties();

    private final List<PropertyMetadata<?>> properties;

    private MemoryTableProperties()
    {
        properties = ImmutableList.of(
                integerProperty(
                        BUCKETS,
                        "Total number of table buckets",
                        8,
                        false),
                stringProperty(
                        BUCKETED_BY,
                        "Columns names used to partition data into different buckets",
                        "",
                        false));
    }

    public List<PropertyMetadata<?>> getProperties()
    {
        return properties;
    }
}
