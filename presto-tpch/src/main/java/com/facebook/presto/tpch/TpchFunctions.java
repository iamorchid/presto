package com.facebook.presto.tpch;

import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class TpchFunctions {

    // 这里创建的函数, 还是放在presto.default命名空间下, 其他connector也可以使用这个函数
    @ScalarFunction("tpch_join")
    @LiteralParameters({"x", "y"})
    @SqlType("varchar")
    public static Slice tpch_join(@SqlType("varchar(x)") Slice s1, @SqlType("varchar(y)") Slice s2) {
        Slice result = Slices.allocate(s1.length() + 1 + s2.length());
        result.setBytes(0, s1);
        result.setBytes(s1.length(), new byte[] { '|' });
        result.setBytes(s1.length() + 1, s2);
        return result;
    }

}
