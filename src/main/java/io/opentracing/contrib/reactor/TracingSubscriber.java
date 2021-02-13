package io.opentracing.contrib.reactor;

import io.opentracing.Span;
import io.opentracing.contrib.reactor.TracingPublishers.SpanDecorator;
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

    public TracingSubscriber(CoreSubscriber<? super T> actual, Span span, @Nullable SpanDecorator decorator) {
        this.actual = actual;
        this.span = span;
        this.decorator = decorator;
        this.context = actual.currentContext().put(Span.class, span);
        this.finished = new AtomicBoolean(false);
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