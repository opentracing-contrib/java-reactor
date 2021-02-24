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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertNull;
import static reactor.core.publisher.SignalType.ON_COMPLETE;
import static reactor.core.publisher.SignalType.ON_SUBSCRIBE;
import static reactor.core.scheduler.Schedulers.boundedElastic;

public class TracedSubscriberTest {
    protected static final MockTracer tracer = new MockTracer(new ThreadLocalScopeManager());

    static final Logger log = Loggers.getLogger(TracedSubscriberTest.class.getSimpleName());


    static <T> Function<? super Publisher<T>, ? extends Publisher<T>> traceWithDefaultDecorator(
        Tracer tracer, String spanName
    ) {
        return Tracing.trace(tracer, spanName, DECORATOR_INSTANCE);
    }

    static <T> Mono<T> logCtxVars(String prefix, Mono<T> source) {
        return source.transformDeferredContextual(
            (m, ctx) -> m.log(prefix + "[" + Stream.ofAll(ctx.stream()).toMap(Tuple::fromEntry) + "]",
                Level.INFO, ON_SUBSCRIBE, ON_COMPLETE));
    }


    private static class DefaultSpanDecorator implements SpanDecorator {
        protected static final String RESULT_KEY = "result";

        @Override
        public SpanBuilder onCreate(Context ctx, SpanBuilder builder) {
            return builder.withTag("someTag", ctx.getOrDefault("someTag", "tag"));
        }

        @Override
        public Span onFinish(Try<SignalType> result, Span span) {
            log.info("Finishing span: {} with {}", span, result);

            return span.log(HashMap.of(RESULT_KEY, result.fold(Throwable::toString, SignalType::toString))
                                   .toJavaMap());
        }
    }

    private static final DefaultSpanDecorator DECORATOR_INSTANCE = new DefaultSpanDecorator();


    @Test
    public void should_pass_tracing_info_when_using_reactor() {
        MockSpan span = tracer.buildSpan("foo").start();
        final AtomicReference<MockSpan> nested1 = new AtomicReference<>();
        final AtomicReference<List<MockSpan>> nested2 = new AtomicReference<>(List.of());
        final AtomicReference<MockSpan> nested3 = new AtomicReference<>();

        // pointless to ask ScopeManager about active span most of the time because onNext signals
        // are emitted on different threads from the one subscribe() was called on (and span was started)
        try (Scope scope = tracer.scopeManager().activate(span)) {
            Flux.just(1, 2, 3)
                .transformDeferredContextual((f, ctx) -> {
                    nested3.set(ctx.getOrDefault(Span.class, null));
                    return f;
                })
                .delayElements(Duration.ofMillis(50))
                .transform(TracedSubscriberTest.<Integer>traceWithDefaultDecorator(tracer, "nested3"))
                .log("source")
                .map(d ->
                    Mono.just(d + 1)
                        .as(m -> logCtxVars("nested2", m))
                        .transformDeferredContextual((m, ctx) -> {
                            nested2.getAndUpdate(
                                list -> list.append(ctx.getOrDefault(Span.class, null)));
                            return m;
                        })
                        .transform(TracedSubscriberTest.<Integer>traceWithDefaultDecorator(tracer, "nested2"))
                        .contextWrite(ctx -> ctx.put("someTag", "tag" + d))
                        .subscribeOn(boundedElastic()))
                .concatMap(m -> m)
                .map(d -> d + 1)
                .collectList()
                .transformDeferredContextual((m, ctx) -> {
                    nested1.set(ctx.getOrDefault(Span.class, null));
                    return m;
                })
                .transform(TracedSubscriberTest.<java.util.List<Integer>>traceWithDefaultDecorator(tracer, "nested1"))
                .log("result")
                .block(Duration.ofSeconds(1));
            // in order for the spans created by Tracing.trace to be children of the outermost one that we
            // create explicitly, outermost subscription must happen on the same thread that it was activated on
        } finally {
            span.finish();
        }

        assertNull(tracer.activeSpan());

        assertThat(nested1.get())
            .isNotNull()
            .satisfies(s -> {
                assertThat(s.operationName()).isEqualTo("nested1");
                assertThat(s.parentId()).isEqualTo(span.context().spanId());
                assertThat(s.context().traceId()).isEqualTo(span.context().traceId());
                assertThat(s.tags().get("someTag")).isEqualTo("tag");
                assertThat(Stream.ofAll(s.logEntries())
                                 .map(le -> le.fields().get(DefaultSpanDecorator.RESULT_KEY))
                                 .filter(Objects::nonNull)
                                 .asJava())
                    .asList()
                    .hasSize(1)
                    .first()
                    .asInstanceOf(InstanceOfAssertFactories.type(String.class))
                    .isEqualTo(ON_COMPLETE.toString());
            });

        assertThat(nested2.get())
            .isNotNull()
            .extracting(List::asJava)
            .asList()
            .hasSize(3)
            .allSatisfy(s ->
                assertThat(s)
                    .isNotNull()
                    .asInstanceOf(InstanceOfAssertFactories.type(MockSpan.class))
                    .extracting(MockSpan::parentId)
                    .isEqualTo(nested1.get().context().spanId()));

        assertThat(nested2.get().map(s -> s.tags().get("someTag")))
            .extracting(List::asJava)
            .asList()
            .containsExactly("tag1", "tag2", "tag3");

        assertThat(nested3.get())
            .isNotNull()
            .extracting(MockSpan::parentId)
            .isEqualTo(nested1.get().context().spanId());
    }

    @Test
    public void should_work_with_errors_too() {
        final AtomicReference<MockSpan> spanRef = new AtomicReference<>();

        Throwable t = Mono
            .<Void>error(new RuntimeException("surprise!"))
            .transformDeferredContextual((f, ctx) -> {
                spanRef.set(ctx.getOrDefault(Span.class, null));
                return f;
            })
            .delaySubscription(Duration.ofMillis(20))
            .transform(TracedSubscriberTest.<Void>traceWithDefaultDecorator(tracer, "trace"))
            .as(m -> Try.of(m::block).failed().get());

        assertThat(spanRef.get())
            .isNotNull()
            .satisfies(s -> {
                long durationMicros = s.finishMicros() - s.startMicros();
                log.info("'{}' span duration: {} mcs", s.operationName(), durationMicros);
                assertThat(durationMicros)
                    .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toMicros(20));
            })
            .extracting(MockSpan::logEntries)
            .satisfies(list ->
                assertThat(Stream.ofAll(list)
                                 .map(le -> le.fields().get(DefaultSpanDecorator.RESULT_KEY))
                                 .filter(Objects::nonNull)
                                 .asJava())
                    .asList()
                    .hasSize(1)
                    .first()
                    .asInstanceOf(InstanceOfAssertFactories.type(String.class))
                    .isEqualTo(t.toString())
            );
    }
}
