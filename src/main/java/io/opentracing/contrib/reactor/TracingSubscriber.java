package io.opentracing.contrib.reactor;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.tag.Tags;
import io.vavr.control.Try;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

public class TracingSubscriber<T> implements SpanSubscription<T> {
    private final CoreSubscriber<? super T> actual;
    private final Span span;
    private final Context context;
    private final SpanDecorator decorator;
    private final AtomicBoolean finished;

    private volatile Subscription subscription;

    public TracingSubscriber(CoreSubscriber<? super T> actual, Tracer tracer, String spanName, String spanKind,
                             @Nullable SpanDecorator decorator) {
        this.actual = actual;
        this.decorator = decorator;
        this.finished = new AtomicBoolean(false);
        Context context = actual.currentContext();

        Span parent = context.<Span>getOrEmpty(Span.class).orElseGet(tracer::activeSpan);

        SpanBuilder spanBuilder = tracer.buildSpan(spanName)
                                        .asChildOf(parent)
                                        .withTag(Tags.SPAN_KIND.getKey(), spanKind);

        this.span = (decorator == null ? spanBuilder : decorator.onCreate(context, spanBuilder)).start();
        this.context = context.put(Span.class, span);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;

        actual.onSubscribe(this);
    }

    @Override
    public void request(long n) {
        subscription.request(n);
    }

    @Override
    public void onNext(T o) {
        actual.onNext(o);
    }

    @Override
    public void cancel() {
        finishSpan(Try.success(SignalType.CANCEL));
        subscription.cancel();
    }

    @Override
    public void onError(Throwable t) {
        finishSpan(Try.failure(t));
        actual.onError(t);
    }

    @Override
    public void onComplete() {
        finishSpan(Try.success(SignalType.ON_COMPLETE));
        actual.onComplete();
    }

    @Override
    public Context currentContext() {
        return context;
    }

    private void finishSpan(Try<SignalType> result) {
        if (finished.compareAndSet(false, true)) {
            (decorator == null ? span : decorator.onFinish(result, span)).finish();
        }
    }
}