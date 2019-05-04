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

import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

/**
 * Based on Spring Sleuth's Reactor instrumentation.
 * A trace representation of the {@link Subscriber}
 *
 * @author Jose Montoya
 */
public class TracedSubscriber<T> implements SpanSubscription<T> {
	private final Span span;
	private final Subscriber<? super T> subscriber;
	private final Context context;
	private final Tracer tracer;
	private Subscription subscription;

	public TracedSubscriber(Subscriber<? super T> subscriber, Context ctx, Tracer tracer) {
		this.subscriber = subscriber;
		this.tracer = tracer;
		this.span = ctx != null ?
				ctx.getOrDefault(Span.class, this.tracer.activeSpan()) : null;

		this.context = ctx != null && this.span != null ?
				ctx.put(Span.class, this.span) : ctx != null ?
				ctx : Context.empty();
	}


	@Override
	public void onSubscribe(Subscription subscription) {
		this.subscription = subscription;
		withActiveSpan(() -> subscriber.onSubscribe(this));
	}

	@Override
	public void request(long n) {
		withActiveSpan(() -> subscription.request(n));
	}

	@Override
	public void onNext(T o) {
		withActiveSpan(() -> subscriber.onNext(o));
	}

	@Override
	public void cancel() {
		withActiveSpan(() -> subscription.cancel());
	}

	@Override
	public void onError(Throwable throwable) {
		subscriber.onError(throwable);
	}

	@Override
	public void onComplete() {
		subscriber.onComplete();
	}

	@Override
	public Context currentContext() {
		return context;
	}

	private void withActiveSpan(Runnable runnable) {
		if (span != null)
			try (Scope inScope = tracer.scopeManager().activate(span)) {
				runnable.run();
			}
		else
			runnable.run();
	}


	/**
	 * Based on Spring Sleuth's Reactor instrumentation.
	 * <p>
	 * Return a span operator pointcut given a {@link Tracer}. This can be used in reactor
	 * via {@link reactor.core.publisher.Flux#transform(Function)}, {@link
	 * reactor.core.publisher.Mono#transform(Function)}, {@link
	 * reactor.core.publisher.Hooks#onEachOperator(Function)} or {@link
	 * reactor.core.publisher.Hooks#onLastOperator(Function)}. The Span operator
	 * pointcut will pass the Scope of the Span without ever creating any new spans.
	 *
	 * @param <T> an arbitrary type that is left unchanged by the span operator
	 * @return a new span operator pointcut
	 */
	public static <T> Function<? super Publisher<T>, ? extends Publisher<T>> asOperator(Tracer tracer) {
		return Operators.liftPublisher(new BiFunction<Publisher, CoreSubscriber<? super T>, CoreSubscriber<? super T>>() {
			@Override
			public CoreSubscriber<? super T> apply(Publisher publisher, CoreSubscriber<? super T> sub) {
				// if Flux/Mono #just, #empty, #error
				if (publisher instanceof Fuseable.ScalarCallable) {
					return sub;
				}

				return new TracedSubscriber<>(sub, sub.currentContext(), tracer);
			}
		});
	}
}