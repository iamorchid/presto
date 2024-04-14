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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

/**
 * 对于{@link GroupingProperty}，group指得是具有相同keys且连续放在一起的一组数据（并不限定是group by以后的结果，
 * 因为group by以后，相同group by keys往往只对应一个结果）。而这里的group里面，可以有多个结果具有相同的keys。
 */
public final class GroupingProperty<E>
        implements LocalProperty<E>
{
    private final Set<E> columns;

    @JsonCreator
    public GroupingProperty(@JsonProperty("columns") Collection<E> columns)
    {
        requireNonNull(columns, "columns is null");

        this.columns = unmodifiableSet(new LinkedHashSet<>(columns));
    }

    @Override
    public boolean isOrderSensitive()
    {
        /**
         * [question] 为啥order会对grouping有影响？
         * order可以优化group的实现，参考{@link #isSimplifiedBy}说明。
         */
        return true;
    }

    @JsonProperty
    public Set<E> getColumns()
    {
        return columns;
    }

    @Override
    public LocalProperty<E> constrain(Set<E> columns)
    {
        if (!this.columns.containsAll(columns)) {
            throw new IllegalArgumentException(String.format("Cannot constrain %s with %s", this, columns));
        }

        return new GroupingProperty<>(columns);
    }

    @Override
    public boolean isSimplifiedBy(LocalProperty<E> known)
    {
        /**
         * 注意如果known为{@link SortingProperty}，它是可以简化group by的。排序本质上也是将数据按照排序的keys进行分组。
         *
         * 如果既有{@link GroupingProperty}，又有{@link SortingProperty}，则排序针对的是同一个group下的items进行排序，
         * 比如窗口函数：f(...) over (partition by clerk order by orderdate)。
         *
         * 参考:
         * {@link com.facebook.presto.sql.planner.optimizations.AddLocalExchanges.Rewriter#AddLocalExchanges}
         * {@link com.facebook.presto.spi.plan.AggregationNode#preGroupedVariables}
         */
        /**
         * ConstantProperty并不一定能简化GroupingProperty，比如{@link #columns}不包含{@link ConstantProperty#column}。
         * 虽然对于known为ConstantProperty时，这里返回了true，但{@link #withConstants}的实现可以看到，如果{@link #columns}
         * 不包含ConstantProperty中的列, 则起不到优化效果。
         *
         * 更合理的做法，这里只用 getColumns().containsAll(known.getColumns()) 即可。
         *
         * 参考：
         * {@link com.facebook.presto.sql.planner.optimizations.LocalProperties#match}
         */
        return known instanceof ConstantProperty || getColumns().containsAll(known.getColumns());
    }

    /**
     * @return Optional.empty() if any of the columns could not be translated
     */
    @Override
    public <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator)
    {
        Set<Optional<T>> translated = columns.stream()
                .map(translator)
                .collect(toCollection(LinkedHashSet::new));

        if (translated.stream().allMatch(Optional::isPresent)) {
            Set<T> columns = translated.stream()
                    .map(Optional::get)
                    .collect(toCollection(LinkedHashSet::new));

            return Optional.of(new GroupingProperty<>(columns));
        }

        return Optional.empty();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("G(");
        builder.append(columns.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ")));
        builder.append(")");
        return builder.toString();
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
        GroupingProperty<?> that = (GroupingProperty<?>) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns);
    }
}
