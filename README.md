# OpenTracing Reactor Instrumentation
OpenTracing instrumentation for Reactor. This instrumentation library is based on [spring-cloud-sleuth's reactor instrumentation](https://github.com/spring-cloud/spring-cloud-sleuth/tree/master/spring-cloud-sleuth-core/src/main/java/org/springframework/cloud/sleuth/instrument/reactor).

## OpenTracing Agents
When using a runtime agent like [java-specialagent](https://github.com/opentracing-contrib/java-specialagent) `TracedSubscriber`s will be automatically added using `Hook.onEachOperator` and `Hooks.onLastOperator`.

Refer to the agent documentation for how to include this library as an instrumentation plugin.

## Non-Agent Configuration
When not using any of the OpenTracing Agents the `Hooks` must be added directly:

```java
Hooks.onEachOperator(TracedSubscriber.asOperator(tracer));
Hooks.onLastOperator(TracedSubscriber.asOperator(tracer));

...
```