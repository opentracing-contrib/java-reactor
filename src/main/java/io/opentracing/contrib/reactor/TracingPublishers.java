package io.opentracing.contrib.reactor;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.tag.Tags;
import io.vavr.control.Try;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

import java.util.function.BiFunction;

/**
 * Contains decorators for both Flux/Mono and their subscribers that allow tracing the lifecycle of selected Publishers.
 * These decorators do obscure wrapped optimizable operators from being recognized by their subscribers
 * so it might be worth it to choose a place for this transformation with a bit of care if there're several steps
 * in the assembly that are subscribed to and complete at roughly the same time
 */
final class TracingPublishers {
    private TracingPublishers() {
    }

    static Span createSpan(Context ctx, Tracer tracer,
                           String spanName, String spanKind,
                           SpanDecorator decorator
    ) {
        Span parent = ctx.<Span>getOrEmpty(Span.class).orElseGet(tracer::activeSpan);

        SpanBuilder spanBuilder = tracer.buildSpan(spanName)
                                        .asChildOf(parent)
                                        .withTag(Tags.SPAN_KIND.getKey(), spanKind);

        return decorator.onCreate(ctx, spanBuilder)
                        .start();
    }


    interface SpanDecorator {
        /**
         * You can decorate a new span with tags or other bells and whistles based on some values
         * passed from downstream via Context, for example
         * It's a good idea to return the same SpanBuilder as was passed to this callback
         */
        SpanBuilder onCreate(Context ctx, SpanBuilder builder);

        /**
         * Similar callback for adding more data to Span upon termination of the Publisher
         * Also a good idea to return the same Span that callback was passed as argument
         */
        Span onFinish(Try<SignalType> result, Span span);
    }

    static final class TracingFlux<E> extends Flux<E> {
        public TracingFlux(Flux<E> upstream, Tracer tracer, String spanName, String spanKind,
                           SpanDecorator decorator) {
            this.upstream = upstream;
            this.tracer = tracer;
            this.spanName = spanName;
            this.spanKind = spanKind;
            this.decorator = decorator;
        }

        private final Flux<E> upstream;
        private final Tracer tracer;
        private final String spanName;
        private final String spanKind;
        private final SpanDecorator decorator;

        /**
         * In order to make a span associated with this publisher available to upstream operators like
         * {@link Flux#transformDeferredContextual(BiFunction)}, it has to be created and put in {@link Context}
         * here rather than in {@link CoreSubscriber#onSubscribe(Subscription)}
         * That callback is invoked after subscriptions are done and by then it's too late to modify the context.
         */
        @Override
        public void subscribe(CoreSubscriber<? super E> actual) {
            Span span = createSpan(actual.currentContext(), tracer, spanName, spanKind, decorator);

            upstream.subscribe(new TracingSubscriber<>(actual, span, decorator));
        }
    }

    /**
     * Using Separate Mono and Flux implementations instead of a single Publisher because otherwise Mono.transform
     * wraps this publisher in Mono.next which cancels subscription after the first element, before it can complete on its own,
     * and TracingSubscriber receives an incorrect termination signal which may be of importance if you're doing something
     * with that signal in {@link SpanDecorator}
     */
    static final class TracingMono<E> extends Mono<E> {
        public TracingMono(Mono<E> upstream, Tracer tracer, String spanName, String spanKind,
                           SpanDecorator decorator) {
            this.upstream = upstream;
            this.tracer = tracer;
            this.spanName = spanName;
            this.spanKind = spanKind;
            this.decorator = decorator;
        }

        private final Mono<E> upstream;
        private final Tracer tracer;
        private final String spanName;
        private final String spanKind;
        private final SpanDecorator decorator;

        @Override
        public void subscribe(CoreSubscriber<? super E> actual) {
            Span span = createSpan(actual.currentContext(), tracer, spanName, spanKind, decorator);

            upstream.subscribe(new TracingSubscriber<>(actual, span, decorator));
        }
    }
}
