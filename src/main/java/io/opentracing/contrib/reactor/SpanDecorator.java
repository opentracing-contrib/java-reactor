package io.opentracing.contrib.reactor;

import io.opentracing.Span;
import io.opentracing.Tracer.SpanBuilder;
import io.vavr.control.Try;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

public interface SpanDecorator {
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
