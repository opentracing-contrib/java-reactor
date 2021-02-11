package io.opentracing.contrib.reactor;

import io.opentracing.Span;
import io.opentracing.contrib.reactor.TracingPublishers.SpanDecorator;
import io.vavr.control.Either;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

public class TracingSubscriber<T> implements SpanSubscription<T> {
    private final CoreSubscriber<? super T> actual;
    private final Span span;
    private final Context context;
    private final SpanDecorator decorator;
    private final AtomicBoolean finished;

    private volatile Subscription subscription;

    public TracingSubscriber(CoreSubscriber<? super T> actual, Span span, SpanDecorator decorator) {
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
        finishSpan(Either.right(SignalType.CANCEL));
        subscription.cancel();
    }

    @Override
    public void onError(Throwable t) {
        finishSpan(Either.left(t));
        actual.onError(t);
    }

    @Override
    public void onComplete() {
        finishSpan(Either.right(SignalType.ON_COMPLETE));
        actual.onComplete();
    }

    @Override
    public Context currentContext() {
        return context;
    }

    private void finishSpan(Either<Throwable, SignalType> result) {
        if (finished.compareAndSet(false, true)) {
            decorator.onFinish(result, span)
                     .finish();
        }
    }
}