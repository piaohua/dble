package com.actiontech.dble.singleton;

import com.actiontech.dble.net.connection.FrontendConnection;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.metrics.Metrics;
import io.jaegertracing.internal.metrics.NoopMetricsFactory;
import io.jaegertracing.internal.reporters.CompositeReporter;
import io.jaegertracing.internal.reporters.LoggingReporter;
import io.jaegertracing.internal.reporters.RemoteReporter;
import io.jaegertracing.internal.samplers.ProbabilisticSampler;
import io.jaegertracing.thrift.internal.senders.HttpSender;
import io.opentracing.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by szf on 2020/5/9.
 */
public class TraceManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheService.class);
    private static final TraceManager INSTANCE = new TraceManager();
    private final JaegerTracer tracer;
    private final Map<FrontendConnection, List<Span>> connectionTracerMap = new ConcurrentHashMap<>();


    private TraceManager() {
        final String endPoint = "http://10.186.60.96:14268/api/traces";

        final CompositeReporter compositeReporter = new CompositeReporter(
                new RemoteReporter.Builder()
                        .withSender(new HttpSender.Builder(endPoint).build())
                        .build(),
                new LoggingReporter()
        );

        final Metrics metrics = new Metrics(new NoopMetricsFactory());

        JaegerTracer.Builder builder = new JaegerTracer.Builder("DBLE")
                .withReporter(compositeReporter)
                .withMetrics(metrics)
                .withExpandExceptionLogs()
                .withSampler(new ProbabilisticSampler(0.5));

        tracer = builder.build();
    }

    public static JaegerTracer getTracer() {
        return INSTANCE.tracer;
    }

    public static void setSpan(FrontendConnection connection, Span newSpan) {
        if (INSTANCE.connectionTracerMap.get(connection) == null) {
            List<Span> spanList = new ArrayList<>();
            spanList.add(newSpan);
            INSTANCE.connectionTracerMap.put(connection, spanList);
        } else {
            List<Span> spanList = INSTANCE.connectionTracerMap.get(connection);
            spanList.add(newSpan);
        }
    }

    public static Span popSpan(FrontendConnection connection, boolean needRemove) {
        List<Span> spanList = INSTANCE.connectionTracerMap.get(connection);
        if (!needRemove) {
            return spanList.get(spanList.size() - 1);
        } else {
            Span span = spanList.get(spanList.size() - 1);
            spanList.remove(span);
            return span;
        }
    }

    public static List<Span> popFullList(FrontendConnection connection) {
        List<Span> spanList = INSTANCE.connectionTracerMap.get(connection);
        INSTANCE.connectionTracerMap.remove(connection);
        return spanList;
    }

    public static void finishList(List<Span> list) {
        for (int i = list.size() - 1; i >= 0; i--) {
            list.get(i).finish();
        }
    }

}
