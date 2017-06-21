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
package org.reaktivity.nukleus.http_push.internal.streams.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.NukleusRule;

public class OpeningIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_push/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_push/streams/proxy/");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("http-push")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        // streams() are still needed due to: https://github.com/k3po/k3po/issues/437
        .streams("http-push", "source")
        .streams("target", "http-push#source")
        .streams("http-push", "target")
        .streams("source", "http-push#target");

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/opening/accept/client",
        "${streams}/opening/connect/server" })
    public void shouldEstablishConnection() throws Exception
    {
        k3po.start();
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/inject.push.promise/accept/client",
        "${streams}/inject.push.promise/connect/server" })
    public void shouldInjectPushPromise() throws Exception
    {
        k3po.start();
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/strip.injected.headers/accept/client",
        "${streams}/strip.injected.headers/connect/server" })
    public void shouldStripInjectPushPromise() throws Exception
    {
        k3po.start();
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/proxy.post.request/accept/client",
        "${streams}/proxy.post.request/connect/server" })
    public void shouldProxyPostRequest() throws Exception
    {
        k3po.start();
        k3po.finish();
    }

}
