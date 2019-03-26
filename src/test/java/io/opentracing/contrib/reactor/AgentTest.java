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
