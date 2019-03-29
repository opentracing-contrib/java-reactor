/*
 * Copyright 2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.reactor;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicReference;

import io.opentracing.Span;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.opentracing.Scope;
import io.opentracing.contrib.specialagent.AgentRunner;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * @author Jose Montoya
 */
@RunWith(AgentRunner.class)
public class AgentTest {
	
	@Test
	public void checkTraceIdFromSubscriberContext(MockTracer tracer) {
		MockSpan initSpan = tracer.buildSpan("foo").start();
		final AtomicReference<Long> spanInSubscriberContext = new AtomicReference<>();

		try (Scope ws = tracer.scopeManager().activate(initSpan, true)) {
			Mono.subscriberContext()
					.map(context -> ((MockSpan) context.get(Span.class)).context().spanId())
					.doOnNext(spanInSubscriberContext::set)
					.block();
		}

		assertEquals((long) spanInSubscriberContext.get(), initSpan.context().spanId()); // ok here
	}

}
