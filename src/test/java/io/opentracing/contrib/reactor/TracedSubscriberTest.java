package io.opentracing.contrib.reactor;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.opentracing.Scope;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Based on Spring Sleuth's Reactor instrumentation
 *
 * @author Jose Montoya
 */
public class TracedSubscriberTest {
	protected static final MockTracer tracer = new MockTracer(new ThreadLocalScopeManager());

	@BeforeClass
	public static void beforeClass() {
		Hooks.onEachOperator(TracedSubscriber.asOperator(tracer));
		Hooks.onLastOperator(TracedSubscriber.asOperator(tracer));
	}

	@Test
	public void should_pass_tracing_info_when_using_reactor() {
		MockSpan span = tracer.buildSpan("foo").start();
		final AtomicReference<MockSpan> spanInOperation = new AtomicReference<>();
		Publisher<Integer> traced = Flux.just(1, 2, 3);

		try (Scope scope = tracer.scopeManager().activate(span, true)) {
			Flux.from(traced)
					.map(d -> d + 1)
					.map(d -> d + 1)
					.map((d) -> {
						spanInOperation.set((MockSpan) tracer.activeSpan());
						return d + 1;
					})
					.map(d -> d + 1)
					.subscribe(System.out::println);
		}

		assertNull(tracer.activeSpan());
		assertEquals(spanInOperation.get().context().traceId(), span.context().traceId());
	}

	@Test
	public void should_support_reactor_fusion_optimization() {
		MockSpan span = tracer.buildSpan("foo").start();
		final AtomicReference<MockSpan> spanInOperation = new AtomicReference<>();

		try (Scope scope = tracer.scopeManager().activate(span, true)) {
			Mono.just(1)
					.flatMap(d -> Flux.just(d + 1)
							.collectList()
							.map(p -> p.get(0)))
					.map(d -> d + 1)
					.map((d) -> {
						spanInOperation.set((MockSpan) tracer.activeSpan());
						return d + 1;
					})
					.map(d -> d + 1)
					.subscribe(System.out::println);
		}

		assertNull(tracer.activeSpan());
		assertEquals(spanInOperation.get().context().traceId(), span.context().traceId());
	}

	@Test
	public void should_not_trace_scalar_flows() {
		MockSpan span = tracer.buildSpan("foo").start();
		final AtomicReference<Subscription> spanInOperation = new AtomicReference<>();

		try (Scope scope = tracer.scopeManager().activate(span, true)) {
			Mono.just(1).subscribe(new BaseSubscriber<Integer>() {
				@Override
				protected void hookOnSubscribe(Subscription subscription) {
					spanInOperation.set(subscription);
				}
			});

			assertNotNull(tracer.activeSpan());
			assertFalse(spanInOperation.get() instanceof TracedSubscriber);

			Mono.<Integer>error(new Exception())
					.subscribe(new BaseSubscriber<Integer>() {
						@Override
						protected void hookOnSubscribe(Subscription subscription) {
							spanInOperation.set(subscription);
						}

						@Override
						protected void hookOnError(Throwable throwable) {
						}
					});

			assertNotNull(tracer.activeSpan());
			assertFalse(spanInOperation.get() instanceof TracedSubscriber);

			Mono.<Integer>empty()
					.subscribe(new BaseSubscriber<Integer>() {
						@Override
						protected void hookOnSubscribe(Subscription subscription) {
							spanInOperation.set(subscription);
						}
					});

			assertNotNull(tracer.activeSpan());
			assertFalse(spanInOperation.get() instanceof TracedSubscriber);
		}

		assertNull(tracer.activeSpan());
	}

	@Test
	public void should_pass_tracing_info_when_using_reactor_async() {
		MockSpan span = tracer.buildSpan("foo").start();
		final AtomicReference<MockSpan> spanInOperation = new AtomicReference<>();

		try (Scope scope = tracer.scopeManager().activate(span, true)) {
			Flux.just(1, 2, 3).publishOn(Schedulers.single()).log("reactor.1")
					.map(d -> d + 1).map(d -> d + 1).publishOn(Schedulers.newSingle("secondThread")).log("reactor.2")
					.map((d) -> {
						spanInOperation.set((MockSpan) tracer.activeSpan());
						return d + 1;
					}).map(d -> d + 1).blockLast();

			Awaitility.await().untilAsserted(() -> {
				assertEquals(spanInOperation.get().context().traceId(), span.context().traceId());
			});

			assertEquals(tracer.activeSpan(), span);
		}

		assertNull(tracer.activeSpan());
		MockSpan foo2 = tracer.buildSpan("foo").start();

		try (Scope ws = tracer.scopeManager().activate(foo2, true)) {
			Flux.just(1, 2, 3).publishOn(Schedulers.single()).log("reactor.").map(d -> d + 1).map(d -> d + 1).map((d) -> {
				spanInOperation.set((MockSpan) tracer.activeSpan());
				return d + 1;
			}).map(d -> d + 1).blockLast();

			assertEquals(tracer.activeSpan(), foo2);
			// parent cause there's an async span in the meantime
			assertEquals(spanInOperation.get().context().traceId(), foo2.context().traceId());
		}

		assertNull(tracer.activeSpan());
	}

	@Test
	public void checkSequenceOfOperations() {
		MockSpan parentSpan = tracer.buildSpan("foo").start();

		try (Scope scope = tracer.scopeManager().activate(parentSpan, true)) {
			final Long traceId = Mono.fromCallable(tracer::activeSpan)
					.map(span -> ((MockSpan) span).context().traceId())
					.block();
			assertNotNull(traceId);

			final Long secondTraceId = Mono.fromCallable(tracer::activeSpan)
					.map(span -> ((MockSpan) span).context().traceId())
					.block();
			assertEquals(secondTraceId, traceId); // different trace ids here
		}
	}

	@Test
	public void checkTraceIdDuringZipOperation() {
		MockSpan initSpan = tracer.buildSpan("foo").start();
		final AtomicReference<Long> spanInOperation = new AtomicReference<>();
		final AtomicReference<Long> spanInZipOperation = new AtomicReference<>();

		try (Scope ws = tracer.scopeManager().activate(initSpan, true)) {
			Mono.fromCallable(tracer::activeSpan)
					.map(span -> ((MockSpan) span).context().traceId())
					.doOnNext(spanInOperation::set)
					.zipWith(
							Mono.fromCallable(tracer::activeSpan)
									.map(span -> ((MockSpan) span).context().traceId())
									.doOnNext(spanInZipOperation::set))
					.block();
		}

		assertEquals((long) spanInZipOperation.get(), initSpan.context().traceId()); // ok here
		assertEquals((long) spanInOperation.get(), initSpan.context().traceId()); // Expecting <AtomicReference[null]> to have value: <1L> but did not.
	}

	// #646
	@Test
	public void should_work_for_mono_just_with_flat_map() {
		MockSpan initSpan = tracer.buildSpan("foo").start();

		try (Scope ws = tracer.scopeManager().activate(initSpan, true)) {
			Mono.just("value1")
					.flatMap(request -> Mono.just("value2")
							.then(Mono.just("foo")))
					.map(a -> "qwe")
					.block();
		}
	}

	// #1030
	@Test
	public void checkTraceIdFromSubscriberContext() {
		MockSpan initSpan = tracer.buildSpan("foo").start();
		final AtomicReference<Long> spanInSubscriberContext = new AtomicReference<>();

		try (Scope ws = tracer.scopeManager().activate(initSpan, true)) {
			Mono.subscriberContext()
					.map(context -> ((MockSpan) tracer.activeSpan()).context().spanId())
					.doOnNext(spanInSubscriberContext::set).block();
		}

		assertEquals((long) spanInSubscriberContext.get(), initSpan.context().spanId()); // ok here
	}

	@AfterClass
	public static void cleanup() {
		Hooks.resetOnLastOperator();
		Hooks.resetOnEachOperator();
		Schedulers.resetFactory();
	}

}
