# OpenTracing Reactor Instrumentation
OpenTracing instrumentation for Reactor. This instrumentation library was once based on [spring-cloud-sleuth's reactor instrumentation](https://github.com/spring-cloud/spring-cloud-sleuth/tree/master/spring-cloud-sleuth-core/src/main/java/org/springframework/cloud/sleuth/instrument/reactor).

Allows wrapping any Mono or Flux in a span that starts on subscription and ends on complete, cancel or error signal.
Reference to an active span is stored in subscription context during subscribe() invocation and the next span upstream 
will use the current one as a parent regardless of what thread its subscribe() method is called on.
Thus, this library is compatible with other integrations like io.opentracing.contrib.spring.web.client.TracingWebClientBeanPostProcessor
which look for the enclosing span in Context at subscribe time. 
When no span is found in Context, falls back to Tracer.activeSpan().

Usage example (see TracedSubscriberTest for more):

```java
    myFlux.transform(f -> new TracingFlux<>(f, tracer, "span name", "span kind", myDecorator))
    
```

Supports pluggable decorator that can augment span with tags, logs, etc. upon creation as well as upon traced publisher's termination.