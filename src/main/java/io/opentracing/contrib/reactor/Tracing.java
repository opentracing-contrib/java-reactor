package io.opentracing.contrib.reactor;

import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Operators;

import java.util.function.Function;

/**
 * @see https://github.com/rsocket/rsocket-rpc-java/tree/master/rsocket-rpc-core/src/main/java/io/rsocket/rpc/tracing
 */
public class Tracing {
    public static <T> Function<? super Publisher<T>, ? extends Publisher<T>> trace(
        Tracer tracer, String spanName, SpanDecorator decorator
    ) {
        return trace(tracer, spanName, Tags.SPAN_KIND_CLIENT, decorator);
    }

    public static <T> Function<? super Publisher<T>, ? extends Publisher<T>> trace(
        Tracer tracer, String spanName, String spanKind, SpanDecorator decorator
    ) {
        return Operators.lift((scannable, subscriber) ->
            new TracingSubscriber<T>(subscriber, tracer, spanName, spanKind, decorator));
    }
}
