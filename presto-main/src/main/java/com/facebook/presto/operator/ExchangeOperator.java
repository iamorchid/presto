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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.RemoteSplit;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ExchangeOperator
        implements SourceOperator, Closeable
{
    public static final ConnectorId REMOTE_CONNECTOR_ID = new ConnectorId("$remote");

    public static class ExchangeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final TaskExchangeClientManager taskExchangeClientManager;
        private final PagesSerdeFactory serdeFactory;
        private ExchangeClient exchangeClient;
        private boolean closed;

        public ExchangeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                TaskExchangeClientManager taskExchangeClientManager,
                PagesSerdeFactory serdeFactory)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.taskExchangeClientManager = requireNonNull(taskExchangeClientManager, "taskExchangeClientManager is null");
            this.serdeFactory = serdeFactory;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, ExchangeOperator.class.getSimpleName());

            /**
             * 虽然ExchangeOperator可以有多个实例（即关联的pipeline创建了多个Driver实例），但ExchangeOperator的多个实例共用
             * 同一个ExchangeClient，通过{@link ExchangeClient#addLocation}实现可以知道，同一个{@link Split}只会被真正读
             * 取一次，但具体数据可以被多个实例并发处理，当然一个{@link Page}仅支持被一个实例处理。
             *
             * 参考：
             * {@link com.facebook.presto.operator.DriverFactory#getDriverInstances()}
             * {@link com.facebook.presto.execution.SqlTaskExecution#scheduleDriversForTaskLifeCycle()}
             */
            if (exchangeClient == null) {
                /**
                 * 这里使用的是pipeline级别的LocalMemoryContext，因为ExchangeClient是同一个pipeline的多个Driver共享的。目前
                 * 来看，pipeline级别的{@link com.facebook.presto.memory.context.LocalMemoryContext}就这里用到。
                 */
                exchangeClient = taskExchangeClientManager.createExchangeClient(driverContext.getPipelineContext().localSystemMemoryContext());
            }

            return new ExchangeOperator(
                    operatorContext,
                    sourceId,
                    serdeFactory.createPagesSerde(),
                    exchangeClient);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeClient exchangeClient;
    private final PagesSerde serde;
    private ListenableFuture<?> isBlocked = NOT_BLOCKED;

    public ExchangeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PagesSerde serde,
            ExchangeClient exchangeClient)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeClient = requireNonNull(exchangeClient, "exchangeClient is null");
        this.serde = requireNonNull(serde, "serde is null");

        operatorContext.setInfoSupplier(exchangeClient::getStatus);
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(ScheduledSplit scheduledSplit)
    {
        Split split = requireNonNull(scheduledSplit, "scheduledSplit is null").getSplit();
        requireNonNull(split, "split is null");
        checkArgument(split.getConnectorId().equals(REMOTE_CONNECTOR_ID), "split is not a remote split");

        RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
        exchangeClient.addLocation(remoteSplit.getLocation().toURI(), remoteSplit.getRemoteSourceTaskId());

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        exchangeClient.noMoreLocations();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return exchangeClient.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        // Avoid registering a new callback in the ExchangeClient when one is already pending
        if (isBlocked.isDone()) {
            isBlocked = exchangeClient.isBlocked();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        SerializedPage page = exchangeClient.pollPage();
        if (page == null) {
            return null;
        }

        operatorContext.recordRawInput(page.getSizeInBytes(), page.getPositionCount());

        Page deserializedPage = serde.deserialize(page);
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), page.getPositionCount());

        return deserializedPage;
    }

    @Override
    public void close()
    {
        exchangeClient.close();
    }
}
