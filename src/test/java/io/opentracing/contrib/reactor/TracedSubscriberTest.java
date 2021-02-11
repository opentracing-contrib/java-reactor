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
import io.opentracing.contrib.reactor.TracingPublishers.SpanDecorator;
import io.opentracing.contrib.reactor.TracingPublishers.TracingFlux;
import io.opentracing.contrib.reactor.TracingPublishers.TracingMono;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.noop.NoopTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Either;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertNull;
import static reactor.core.publisher.SignalType.ON_COMPLETE;
import static reactor.core.publisher.SignalType.ON_NEXT;
import static reactor.core.publisher.SignalType.ON_SUBSCRIBE;
import static reactor.core.scheduler.Schedulers.boundedElastic;

public class TracedSubscriberTest {
    protected static final MockTracer tracer = new MockTracer(new ThreadLocalScopeManager());

    static final Logger log = Loggers.getLogger(TracedSubscriberTest.class.getSimpleName());


    public static <E> Function<? super Flux<E>, ? extends Publisher<E>> tracedFlux(
        Tracer tracer, String spanName
    ) {
        return tracer instanceof NoopTracer ?
            Function.identity() :
            f -> new TracingFlux<>(f, tracer, spanName, "zero or more of a kind", DECORATOR_INSTANCE);
    }

    public static <E> Function<? super Mono<E>, ? extends Publisher<E>> tracedMono(
        Tracer tracer, String spanName
    ) {
        return tracer instanceof NoopTracer ?
            Function.identity() :
            f -> new TracingMono<>(f, tracer, spanName, "one (or zero) of a kind", DECORATOR_INSTANCE);
    }


    static <T> Flux<T> logCtxVars(String prefix, Flux<T> source) {
        return source.transformDeferredContextual(
            (f, cv) -> f.log(prefix + "[" + Stream.ofAll(cv.stream()).toMap(Tuple::fromEntry) + "]",
                Level.INFO, ON_SUBSCRIBE, ON_NEXT, ON_COMPLETE));
    }

    static <T> Mono<T> logCtxVars(String prefix, Mono<T> source) {
        return source.transformDeferredContextual(
            (m, ctx) -> m.log(prefix + "[" + Stream.ofAll(ctx.stream()).toMap(Tuple::fromEntry) + "]",
                Level.INFO, ON_SUBSCRIBE, ON_COMPLETE));
    }


    private static class DefaultSpanDecorator implements SpanDecorator {
        @Override
        public SpanBuilder onCreate(Context ctx, SpanBuilder builder) {
            // SpanBuilder's toString is useless and it doesn't expose its references
//            log.info("Creating span: {}", builder);

            return builder.withTag("someTag", ctx.getOrDefault("someTag", "tag"));
        }

        @Override
        public Span onFinish(Either<Throwable, SignalType> result, Span span) {
            log.info("Finishing span: {}", span);

            return span.log(HashMap.of("result", result.fold(Throwable::toString, SignalType::toString))
                                   .toJavaMap());
        }
    }

    private static final DefaultSpanDecorator DECORATOR_INSTANCE = new DefaultSpanDecorator();


    @Test
    public void should_pass_tracing_info_when_using_reactor() {
        MockSpan span = tracer.buildSpan("foo").start();
        final AtomicReference<MockSpan> nested1 = new AtomicReference<>();
        final AtomicReference<List<MockSpan>> nested2 = new AtomicReference<>(List.of());

        // pointless to ask ScopeManager about active span most of the time because onNext signals
        // are emitted on different threads from the one subscribe() was called on (and span was started)
        try (Scope scope = tracer.scopeManager().activate(span)) {
            Flux.just(1, 2, 3)
                .delayElements(Duration.ofMillis(50))
                .log("source")
                .map(d ->
                    Mono.just(d + 1)
                        .as(m -> logCtxVars("nested2", m))
                        .transformDeferredContextual((m, ctx) -> {
                            nested2.getAndUpdate(
                                list -> list.append(ctx.getOrDefault(Span.class, null)));
                            return m;
                        })
                        .transform(TracedSubscriberTest.<Integer>tracedMono(tracer, "nested2"))
                        .contextWrite(ctx -> ctx.put("someTag", "tag" + d))
                        .subscribeOn(boundedElastic()))
                .concatMap(m -> m)
                .map(d -> d + 1)
                .collectList()
                .transformDeferredContextual((m, ctx) -> {
                    nested1.set(ctx.getOrDefault(Span.class, null));
                    return m;
                })
                .transform(TracedSubscriberTest.<java.util.List<Integer>>tracedMono(tracer, "nested1"))
                .log("result")
                .block(Duration.ofSeconds(1));
            // in order for the spans created by TracingPublishers to be children of the outermost one that we
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
    }
}
