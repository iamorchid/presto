package com.facebook.presto.example.functions;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.*;
import io.airlift.slice.Slice;

public class ExampleFunctions {

    @ScalarFunction(value = "len", calledOnNullInput = true)
    @Description("Returns TRUE if the argument is NULL")
    @SqlType(StandardTypes.INTEGER)
    public static long len(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        return (string != null) ? string.toStringUtf8().length() : 0;
    }

}
