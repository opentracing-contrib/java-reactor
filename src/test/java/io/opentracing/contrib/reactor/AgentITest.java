package io.opentracing.contrib.reactor;

import io.opentracing.Scope;
import io.opentracing.contrib.specialagent.AgentRunner;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import org.junit.Test;
import org.junit.runner.RunWith;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * @author Jose Montoya
 */
@RunWith(AgentRunner.class)
@AgentRunner.Config(debug = true, verbose = true)
public class AgentITest {

	// #1030
	@Test
	public void checkTraceIdFromSubscriberContext(MockTracer tracer) {
		MockSpan initSpan = tracer.buildSpan("foo").start();
		final AtomicReference<Long> spanInSubscriberContext = new AtomicReference<>();

		try (Scope ws = tracer.scopeManager().activate(initSpan, true)) {
			Mono.subscriberContext()
					.map(context -> ((MockSpan) tracer.activeSpan()).context().spanId())
					.doOnNext(spanInSubscriberContext::set).block();
		}

		assertEquals((long) spanInSubscriberContext.get(), initSpan.context().spanId()); // ok here
	}
}
