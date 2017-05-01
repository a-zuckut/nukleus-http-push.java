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

import static org.reaktivity.nukleus.http.push.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.http.push.internal.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE;
import static org.reaktivity.nukleus.http.push.internal.util.HttpHeadersUtil.INJECTED_HEADER_NAME;
import static org.reaktivity.nukleus.http.push.internal.util.HttpHeadersUtil.IS_POLL_HEADER;
import static org.reaktivity.nukleus.http.push.internal.util.HttpHeadersUtil.forAllMatch;

//import java.util.LinkedHashMap;
import java.util.List;
//import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http.push.internal.routable.Route;
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
import org.reaktivity.nukleus.http.push.internal.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http.push.internal.util.function.LongObjectBiConsumer;

public final class SourceInputStreamFactory
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final OctetsFW octetsRO = new OctetsFW();

    private final LongObjectBiConsumer<Runnable> scheduler;
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyTargetId;
    private final LongObjectBiConsumer<Correlation> correlateNew;
    private final Slab slab;

    private static final Predicate<? super HttpHeaderFW> IS_INJECTED_HEADER =
            h -> INJECTED_HEADER_NAME.equals(h.name().asString());

    public SourceInputStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyTargetId,
        LongObjectBiConsumer<Correlation> correlateNew,
        Slab slab,
        LongObjectBiConsumer<Runnable> scheduler)
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyTargetId = supplyTargetId;
        this.correlateNew = correlateNew;
        this.slab = slab;
        this.scheduler = scheduler;
    }

    public MessageHandler newStream()
    {
        return new SourceInputStream()::handleStream;
    }

    private final class SourceInputStream
    {
        private MessageHandler streamState;

        private long sourceId;
        private Target target;
        private long targetId;

        // these are fields do to effectively final forEach
        private int storedRequestSize = 0;
        private int pollInterval = 0;
        private int pollExtensionSize = 0;

        private SourceInputStream()
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

        private void afterReplyOrReset(
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

            this.streamState = this::afterReplyOrReset;
        }

        private void processInvalidRequest(
            DirectBuffer buffer,
            int index,
            int length,
            long sourceRef,
            String status)
        {
            final Optional<Route> optional = resolveReplyTo(sourceRef);

            if (optional.isPresent())
            {
                final Route route = optional.get();
                final Target replyTo = route.target();
                final long newTargetId = supplyTargetId.getAsLong();

                replyTo.doHttpEnd(newTargetId);

                this.streamState = this::afterReplyOrReset;
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);

            final long newSourceId = beginRO.streamId();
            final long sourceRef = beginRO.referenceId();
            final long correlationId = beginRO.correlationId();

            {
                final Optional<Route> optional = resolveTarget(sourceRef);

                if (optional.isPresent())
                {
                    final long newTargetId = supplyTargetId.getAsLong();
                    final long targetCorrelationId = newTargetId;

                    final Route route = optional.get();
                    final Target newTarget = route.target();
                    final long targetRef = route.targetRef();
                    final long streamId = beginRO.streamId();
                    final OctetsFW extension = beginRO.extension();
                    extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> headers = httpBeginExRO.headers();

                    int slotIndex = slab.acquire(streamId);
                    final MutableDirectBuffer store = slab.buffer(slotIndex);
                    storeHeadersForTargetEstablish(headers, store);

                    if(headers.anyMatch(IS_POLL_HEADER) && headers.anyMatch(IS_INJECTED_HEADER))
                    {
                        forAllMatch(headers, IS_POLL_HEADER, h ->
                        {
                            this.pollInterval = Integer.parseInt(h.value().asString());
                        });
                        schedulePoll(newTargetId, targetCorrelationId, newTarget, targetRef, streamId);
                    }
                    else
                    {
                        newTarget.doHttpBegin(newTargetId, targetRef, targetCorrelationId, beginRO.extension());
                        newTarget.addThrottle(newTargetId, this::handleThrottle);

                    }

                    final Correlation correlation = new Correlation(correlationId, source.routableName(),
                            OUTPUT_ESTABLISHED, slotIndex, this.storedRequestSize, slab);
                    correlateNew.accept(targetCorrelationId, correlation);

                    this.sourceId = newSourceId;
                    this.target = newTarget;
                    this.targetId = newTargetId;
                }
                else
                {
                    processInvalidRequest(buffer, index, length, sourceRef, "400");
                }
            }

            this.streamState = this::afterBeginOrData;
        }

        private void storeHeadersForTargetEstablish(ListFW<HttpHeaderFW> headers, final MutableDirectBuffer store)
        {
            HttpHeadersUtil.forAllMatch(headers, b -> true, h ->
            {
                store.putBytes(storedRequestSize, h.buffer(), h.offset(), h.sizeof());
                this.storedRequestSize += h.sizeof();
            });
        }

        private void schedulePoll(final long newTargetId, final long targetCorrelationId, final Target newTarget,
                final long targetRef, final long streamId)
        {
            final int slotIndex = slab.acquire(streamId + 5000);
            final MutableDirectBuffer store = slab.buffer(slotIndex);

            beginRO.extension().get(httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> headers = httpBeginExRO.headers();

            Predicate<HttpHeaderFW> stripInjected = h -> INJECTED_HEADER_NAME.equals(h.name().asString());
            if(headers.anyMatch(INJECTED_HEADER_AND_NO_CACHE))
            {
                if(headers.anyMatch(HttpHeadersUtil.NO_CACHE_CACHE_CONTROL))
                {
                    stripInjected = stripInjected.or(h -> "cache-control".equals(h.name().asString()));
                }
                else
                {
                    // figure out how to remove just cache-control: no-cache and not all directives
                    System.out.println("TODO");
                    throw new RuntimeException("TODO");
                }
            }


            HttpHeadersUtil.forAllMatch(headers, stripInjected.negate(), h ->
            {
                store.putBytes(pollExtensionSize, h.buffer(), h.offset(), h.sizeof());
                pollExtensionSize += h.sizeof();
            });

            scheduler.accept(System.currentTimeMillis() + (pollInterval * 1000), () ->
            {
                octetsRO.wrap(store, 0, pollExtensionSize);
                newTarget.doHttpBegin(newTargetId, targetRef, targetCorrelationId, octetsRO);
                newTarget.addThrottle(newTargetId, this::handleThrottle);
                slab.release(slotIndex);
            });
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);

            final OctetsFW payload = dataRO.payload();

            processPayload(payload);
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            final long streamId = endRO.streamId();
            this.streamState = this::afterEnd;

            source.removeStream(streamId);
            target.removeThrottle(targetId);
        }

        private int processPayload(
            final OctetsFW httpPayload)
        {
            // DPW TODO
//            final DirectBuffer buffer = httpPayload.buffer();
//            final int offset = httpPayload.offset();
//            final int limit = httpPayload.limit();
            return 0;
        }

        private Optional<Route> resolveTarget(
            long sourceRef)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);

            return routes.stream().findFirst();
        }

        private Optional<Route> resolveReplyTo(
            long sourceRef)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);
            final Predicate<Route> predicate = Route.sourceMatches(source.routableName());

            return routes.stream().filter(predicate).findFirst();
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

            final int update = windowRO.update();

            source.doWindow(sourceId, update);
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
