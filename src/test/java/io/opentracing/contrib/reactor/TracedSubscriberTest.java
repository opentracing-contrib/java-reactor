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
import static reactor.core.scheduler.Schedulers.elastic;

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

		try (Scope scope = tracer.scopeManager().activate(span)) {
			Flux.from(traced)
					.map(d -> d + 1)
					.map(d -> d + 1)
					.map((d) -> {
						spanInOperation.set((MockSpan) tracer.activeSpan());
						return d + 1;
					})
					.map(d -> d + 1)
					.subscribe(System.out::println);
		} finally {
			span.finish();
		}

		assertNull(tracer.activeSpan());
		assertEquals(spanInOperation.get().context().traceId(), span.context().traceId());
	}

	@Test
	public void should_support_reactor_fusion_optimization() {
		MockSpan span = tracer.buildSpan("foo").start();
		final AtomicReference<MockSpan> spanInOperation = new AtomicReference<>();

		try (Scope scope = tracer.scopeManager().activate(span)) {
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
		} finally {
			span.finish();
		}

		assertNull(tracer.activeSpan());
		assertEquals(spanInOperation.get().context().traceId(), span.context().traceId());
	}

	@Test
	public void should_not_trace_scalar_flows() {
		MockSpan span = tracer.buildSpan("foo").start();
		final AtomicReference<Subscription> spanInOperation = new AtomicReference<>();

		try (Scope scope = tracer.scopeManager().activate(span)) {
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
		} finally {
			span.finish();
		}

		assertNull(tracer.activeSpan());
	}

	@Test
	public void should_pass_tracing_info_when_using_reactor_async() {
		MockSpan span = tracer.buildSpan("foo").start();
		final AtomicReference<MockSpan> spanInOperation = new AtomicReference<>();

		try (Scope scope = tracer.scopeManager().activate(span)) {
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
		} finally {
			span.finish();
		}

		assertNull(tracer.activeSpan());
		MockSpan foo2 = tracer.buildSpan("foo").start();

		try (Scope ws = tracer.scopeManager().activate(foo2)) {
			Flux.just(1, 2, 3).publishOn(Schedulers.single()).log("reactor.").map(d -> d + 1).map(d -> d + 1).map((d) -> {
				spanInOperation.set((MockSpan) tracer.activeSpan());
				return d + 1;
			}).map(d -> d + 1).blockLast();

			assertEquals(tracer.activeSpan(), foo2);
			// parent cause there's an async span in the meantime
			assertEquals(spanInOperation.get().context().traceId(), foo2.context().traceId());
		} finally {
			foo2.finish();
		}

		assertNull(tracer.activeSpan());
	}

	@Test
	public void checkSequenceOfOperations() {
		MockSpan parentSpan = tracer.buildSpan("foo").start();

		try (Scope scope = tracer.scopeManager().activate(parentSpan)) {
			final Long traceId = Mono.fromCallable(tracer::activeSpan)
					.map(span -> ((MockSpan) span).context().traceId())
					.block();
			assertNotNull(traceId);

			final Long secondTraceId = Mono.fromCallable(tracer::activeSpan)
					.map(span -> ((MockSpan) span).context().traceId())
					.block();
			assertEquals(secondTraceId, traceId); // different trace ids here
		} finally {
			parentSpan.finish();
		}
	}

	@Test
	public void checkTraceIdDuringZipOperation() {
		MockSpan initSpan = tracer.buildSpan("foo").start();
		final AtomicReference<Long> spanInOperation = new AtomicReference<>();
		final AtomicReference<Long> spanInZipOperation = new AtomicReference<>();

		try (Scope ws = tracer.scopeManager().activate(initSpan)) {
			Mono.fromCallable(tracer::activeSpan)
					.map(span -> ((MockSpan) span).context().traceId())
					.doOnNext(spanInOperation::set)
					.zipWith(
							Mono.fromCallable(tracer::activeSpan)
									.map(span -> ((MockSpan) span).context().traceId())
									.doOnNext(spanInZipOperation::set))
					.block();
		} finally {
			initSpan.finish();
		}

		assertEquals((long) spanInZipOperation.get(), initSpan.context().traceId()); // ok here
		assertEquals((long) spanInOperation.get(), initSpan.context().traceId()); // Expecting <AtomicReference[null]> to have value: <1L> but did not.
	}

	// #646
	@Test
	public void should_work_for_mono_just_with_flat_map() {
		MockSpan initSpan = tracer.buildSpan("foo").start();

		try (Scope ws = tracer.scopeManager().activate(initSpan)) {
			Mono.just("value1")
					.flatMap(request -> Mono.just("value2")
							.then(Mono.just("foo")))
					.map(a -> "qwe")
					.block();
		} finally {
			initSpan.finish();
		}
	}

	// #1030
	@Test
	public void checkTraceIdFromSubscriberContext() {
		MockSpan initSpan = tracer.buildSpan("foo").start();
		final AtomicReference<Long> spanInSubscriberContext = new AtomicReference<>();

		try (Scope ws = tracer.scopeManager().activate(initSpan)) {
			Mono.subscriberContext()
					.map(context -> ((MockSpan) tracer.activeSpan()).context().spanId())
					.doOnNext(spanInSubscriberContext::set).block();
		} finally {
			initSpan.finish();
		}

		assertEquals((long) spanInSubscriberContext.get(), initSpan.context().spanId()); // ok here
	}

	@Test
	public void activeSpanShouldBeAccessibleInOnCompleteCallback() {
		MockSpan initSpan = tracer.buildSpan("foo").start();

		try (Scope ws = tracer.scopeManager().activate(initSpan)) {
			Flux.range(1, 5)
				.flatMap(i -> Mono.fromCallable(() -> i * 2).subscribeOn(elastic()))
				.doOnComplete(() -> assertNotNull(tracer.activeSpan()))
				.then()
				.block();
		} finally {
			initSpan.finish();
		}
	}

	@Test
	public void activeSpanShouldBeAccessibleInOnErrorCallback() {
		MockSpan initSpan = tracer.buildSpan("foo").start();

		try (Scope ws = tracer.scopeManager().activate(initSpan)) {
			Mono.error(RuntimeException::new)
				.subscribeOn(elastic())
				.doOnError(e -> assertNotNull(tracer.activeSpan()))
				.onErrorResume(RuntimeException.class, e -> Mono.just("fallback"))
				.block();
		} finally {
			initSpan.finish();
		}
	}

	@AfterClass
	public static void cleanup() {
		Hooks.resetOnLastOperator();
		Hooks.resetOnEachOperator();
		Schedulers.resetFactory();
	}

}
