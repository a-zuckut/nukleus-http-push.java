/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http.push.internal.routable.stream;

import static org.reaktivity.nukleus.http.push.internal.routable.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.http.push.internal.util.HttpHeadersUtil.IS_POLL_HEADER;

import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http.push.internal.routable.Source;
import org.reaktivity.nukleus.http.push.internal.routable.Target;
import org.reaktivity.nukleus.http.push.internal.router.Correlation;
import org.reaktivity.nukleus.http.push.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http.push.internal.types.ListFW;
import org.reaktivity.nukleus.http.push.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.push.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.push.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.push.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.push.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.push.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.push.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.push.internal.types.stream.WindowFW;

public final class TargetOutputEstablishedStreamFactory
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final ListFW<HttpHeaderFW> headersFW = new HttpBeginExFW().headers();
    private final HttpBeginExFW httpBeginExFW = new HttpBeginExFW();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final Function<String, Target> supplyTarget;
    private final LongSupplier supplyStreamId;
    private final LongFunction<Correlation> correlateEstablished;

    public TargetOutputEstablishedStreamFactory(
        Source source,
        Function<String, Target> supplyTarget,
        LongSupplier supplyStreamId,
        LongFunction<Correlation> correlateEstablished)
    {
        this.source = source;
        this.supplyTarget = supplyTarget;
        this.supplyStreamId = supplyStreamId;
        this.correlateEstablished = correlateEstablished;
    }

    public MessageHandler newStream()
    {
        return new TargetOutputEstablishedStream()::handleStream;
    }

    private final class TargetOutputEstablishedStream
    {
        private MessageHandler streamState;
        boolean lockThrottle = false;
        int throttleDebt = 0;

        private long sourceId;

        private Target target;
        private long targetId;

        private TargetOutputEstablishedStream()
        {
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            streamState.onMessage(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                processBegin(buffer, index, length);
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void afterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                processData(buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                processEnd(buffer, index, length);
                break;
            default:
                processUnexpected(buffer, index, length);
                break;
            }
        }

        private void afterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            processUnexpected(buffer, index, length);
        }

        private void afterRejectOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                final long streamId = dataRO.streamId();

                source.doWindow(streamId, length);
            }
            else if (msgTypeId == EndFW.TYPE_ID)
            {
                endRO.wrap(buffer, index, index + length);
                final long streamId = endRO.streamId();

                source.removeStream(streamId);

                this.streamState = this::afterEnd;
            }
        }

        private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);

            final long streamId = frameRO.streamId();

            source.doReset(streamId);

            this.streamState = this::afterRejectOrReset;
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);

            final long newSourceId = beginRO.streamId();
            final long sourceRef = beginRO.referenceId();
            final long targetCorrelationId = beginRO.correlationId();

            final Correlation correlation = correlateEstablished.apply(targetCorrelationId);

            if (sourceRef == 0L && correlation != null)
            {
                final Target newTarget = supplyTarget.apply(correlation.source());
                final long newTargetId = supplyStreamId.getAsLong();
                final long sourceCorrelationId = correlation.id();

                final OctetsFW extension = beginRO.extension();
                final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                // TODO, want match once
                httpBeginEx.headers().forEach(header ->
                {
                    final String name = header.name().asString();
                    final String value = header.value().asString();
                    if ("cache-control".equalsIgnoreCase(name))
                    {
                        // TODO, do I need flyweight?? else a real matcher
                        if(value.contains("no-cache"))
                        {
//                            TODO 304 round trip
                        }
                    }
                });

                newTarget.doHttpBegin(newTargetId, 0L, sourceCorrelationId, beginRO.extension());
                int slabIndex = correlation.slabIndex();
                if(slabIndex != NO_SLOT)
                {
                    MutableDirectBuffer h2PushBuffer = correlation.slab().buffer(slabIndex);
                    headersFW.wrap(h2PushBuffer, 0, correlation.slabSlotLimit());
                    if (headersFW.anyMatch(IS_POLL_HEADER))
                    {
                        this.throttleDebt += newTarget.doH2PushPromise(newTargetId, headersFW, x -> headersFW
                            .forEach(h -> x.item(y ->
                            {
                                y.representation((byte) 0).name(h.name()).value(h.value());
                            })));
                        unlockThrottle();
                    }
                }

                newTarget.addThrottle(newTargetId, this::handleThrottle);

                this.sourceId = newSourceId;
                this.target = newTarget;
                this.targetId = newTargetId;

                this.streamState = this::afterBeginOrData;
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);
            target.doHttpData(targetId, dataRO.payload());
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);

            target.doHttpEnd(targetId);
            target.removeThrottle(targetId);
            source.removeStream(sourceId);
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);
            if(this.lockThrottle)
            {
                int update = windowRO.update();
                this.throttleDebt -= update;
            }
            else if (this.throttleDebt > 0)
            {
                int update = windowRO.update();
                update = update - this.throttleDebt;
                if (update >= 0)
                {
                    source.doWindow(sourceId, update);
                    this.throttleDebt = 0;
                }
                else
                {
                    this.throttleDebt = Math.abs(this.throttleDebt);
                }
            }
            else
            {
                source.doWindow(sourceId, windowRO.update());
            }
        }

        private void unlockThrottle()
        {
            this.lockThrottle = false;
            if(this.throttleDebt < 0)
            {
                System.out.println("Unlocked Throttle sent Window");
                source.doWindow(sourceId, Math.abs(this.throttleDebt));
                this.throttleDebt = 0;
            }
        }

        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);

            source.doReset(sourceId);
        }
    }
}
