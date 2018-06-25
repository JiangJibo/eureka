/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.shared.transport.decorator;

import com.netflix.discovery.EurekaClientNames;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpClientFactory;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import com.netflix.discovery.shared.transport.TransportClientFactory;
import com.netflix.discovery.shared.transport.decorator.MetricsCollectingEurekaHttpClient.EurekaHttpClientRequestMetrics.Status;
import com.netflix.discovery.util.ExceptionsMetric;
import com.netflix.discovery.util.ServoUtil;
import com.netflix.servo.monitor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Metrics： ['metrɪks] 指标, 指标收集Eureka Http Client, 延迟，连接错误，执行异常等指标
 *
 * @author Tomasz Bak
 */
public class MetricsCollectingEurekaHttpClient extends EurekaHttpClientDecorator {

    private static final Logger logger = LoggerFactory.getLogger(MetricsCollectingEurekaHttpClient.class);

    /**
     * 转发HttpClient对象
     */
    private final EurekaHttpClient delegate;

    /**
     * 每一种请求类型与指标对象的映射
     */
    private final Map<RequestType, EurekaHttpClientRequestMetrics> metricsByRequestType;
    /**
     * 异常指标
     */
    private final ExceptionsMetric exceptionsMetric;
    /**
     * 关闭指标
     */
    private final boolean shutdownMetrics;

    public MetricsCollectingEurekaHttpClient(EurekaHttpClient delegate) {
        this(delegate, initializeMetrics(), new ExceptionsMetric(EurekaClientNames.METRIC_TRANSPORT_PREFIX + "exceptions"), true);
    }

    /**
     * @param delegate
     * @param metricsByRequestType 请求类型与指标的映射
     * @param exceptionsMetric
     * @param shutdownMetrics
     */
    private MetricsCollectingEurekaHttpClient(EurekaHttpClient delegate,
                                              Map<RequestType, EurekaHttpClientRequestMetrics> metricsByRequestType,
                                              ExceptionsMetric exceptionsMetric,
                                              boolean shutdownMetrics) {
        this.delegate = delegate;
        this.metricsByRequestType = metricsByRequestType;
        this.exceptionsMetric = exceptionsMetric;
        this.shutdownMetrics = shutdownMetrics;
    }

    @Override
    protected <R> EurekaHttpResponse<R> execute(RequestExecutor<R> requestExecutor) {
        // 获得 请求类型 的 请求指标
        EurekaHttpClientRequestMetrics requestMetrics = metricsByRequestType.get(requestExecutor.getRequestType());
        // 开启延迟指标记录监视器
        Stopwatch stopwatch = requestMetrics.latencyTimer.start();
        try {
            // 执行请求
            EurekaHttpResponse<R> httpResponse = requestExecutor.execute(delegate);
            // 增加 请求指标, 根据状态码自增相应的指标记录器
            requestMetrics.countersByStatus.get(mappedStatus(httpResponse)).increment();
            return httpResponse;
        } catch (Exception e) {
            // 连接异常指标数值+1
            requestMetrics.connectionErrors.increment();
            // 若此异常对应的异常指标不存在,创建一个
            exceptionsMetric.count(e);
            throw e;
        } finally {
            // TimedStopwatch , 记录延迟时间
            stopwatch.stop();
        }
    }

    @Override
    public void shutdown() {
        if (shutdownMetrics) {
            shutdownMetrics(metricsByRequestType);
            exceptionsMetric.shutdown();
        }
    }

    public static EurekaHttpClientFactory createFactory(final EurekaHttpClientFactory delegateFactory) {
        final Map<RequestType, EurekaHttpClientRequestMetrics> metricsByRequestType = initializeMetrics();
        final ExceptionsMetric exceptionMetrics = new ExceptionsMetric(EurekaClientNames.METRIC_TRANSPORT_PREFIX + "exceptions");
        return new EurekaHttpClientFactory() {
            @Override
            public EurekaHttpClient newClient() {
                return new MetricsCollectingEurekaHttpClient(
                    delegateFactory.newClient(),
                    metricsByRequestType,
                    exceptionMetrics,
                    false
                );
            }

            @Override
            public void shutdown() {
                shutdownMetrics(metricsByRequestType);
                exceptionMetrics.shutdown();
            }
        };
    }

    /**
     * @param delegateFactory
     * @return
     */
    public static TransportClientFactory createFactory(final TransportClientFactory delegateFactory) {
        // 初始化指标,为每种请求类型创建现行的指标对象
        final Map<RequestType, EurekaHttpClientRequestMetrics> metricsByRequestType = initializeMetrics();
        final ExceptionsMetric exceptionMetrics = new ExceptionsMetric(EurekaClientNames.METRIC_TRANSPORT_PREFIX + "exceptions");
        return new TransportClientFactory() {
            @Override
            public EurekaHttpClient newClient(EurekaEndpoint endpoint) {
                return new MetricsCollectingEurekaHttpClient(
                    delegateFactory.newClient(endpoint),
                    metricsByRequestType,
                    exceptionMetrics,
                    false
                );
            }

            @Override
            public void shutdown() {
                shutdownMetrics(metricsByRequestType);
                exceptionMetrics.shutdown();
            }
        };
    }

    /**
     * 初始化指标,为每种请求类型创建现行的指标对象
     *
     * @return
     */
    private static Map<RequestType, EurekaHttpClientRequestMetrics> initializeMetrics() {
        // 请求类型枚举和请求指标对象间的映射, 比如 RequestType.Register   >>  new EurekaHttpClientRequestMetrics("Register")
        Map<RequestType, EurekaHttpClientRequestMetrics> result = new EnumMap<>(RequestType.class);
        try {
            for (RequestType requestType : RequestType.values()) {
                result.put(requestType, new EurekaHttpClientRequestMetrics(requestType.name()));
            }
        } catch (Exception e) {
            logger.warn("Metrics initialization failure", e);
        }
        return result;
    }

    private static void shutdownMetrics(Map<RequestType, EurekaHttpClientRequestMetrics> metricsByRequestType) {
        for (EurekaHttpClientRequestMetrics metrics : metricsByRequestType.values()) {
            metrics.shutdown();
        }
    }

    private static Status mappedStatus(EurekaHttpResponse<?> httpResponse) {
        int category = httpResponse.getStatusCode() / 100;
        switch (category) {
            case 1:
                return Status.x100;
            case 2:
                return Status.x200;
            case 3:
                return Status.x300;
            case 4:
                return Status.x400;
            case 5:
                return Status.x500;
        }
        return Status.Unknown;
    }

    /**
     * Http请求的指标对象, {@link RequestType} 每一个请求类型对应一个指标对象
     */
    static class EurekaHttpClientRequestMetrics {

        /**
         * 请求返回的状态码
         */
        enum Status {
            x100,
            x200,
            x300,
            x400,
            x500,
            Unknown
        }

        private final Timer latencyTimer;
        private final Counter connectionErrors;
        private final Map<Status, Counter> countersByStatus;

        /**
         * @param resourceName 比如 "Register"
         */
        EurekaHttpClientRequestMetrics(String resourceName) {
            this.countersByStatus = createStatusCounters(resourceName);

            latencyTimer = new BasicTimer(
                MonitorConfig.builder(EurekaClientNames.METRIC_TRANSPORT_PREFIX + "latency") // 延迟： "eurekaClient.transport.latency"
                    .withTag("id", resourceName)                                               //  "Register"
                    .withTag("class", MetricsCollectingEurekaHttpClient.class.getSimpleName())
                    .build(),
                TimeUnit.MILLISECONDS
            );
            // JmxMonitorRegistry 注册 延迟监视器
            ServoUtil.register(latencyTimer);

            this.connectionErrors = new BasicCounter(
                MonitorConfig.builder(EurekaClientNames.METRIC_TRANSPORT_PREFIX + "connectionErrors") // 连接错误： "eurekaClient.transport.connectionErrors"
                    .withTag("id", resourceName)                                                        //  "Register"
                    .withTag("class", MetricsCollectingEurekaHttpClient.class.getSimpleName())
                    .build()
            );
            // JmxMonitorRegistry 注册 连接错误监视器
            ServoUtil.register(connectionErrors);
        }

        void shutdown() {
            ServoUtil.unregister(latencyTimer, connectionErrors);
            ServoUtil.unregister(countersByStatus.values());
        }

        /**
         * 创建状态与数值检测器间的映射关系
         * 为一个{@link RequestType}的枚举类型创建其与{@link Status}内每一个枚举实例的映射关系
         *
         * @param resourceName 比如 "Register"
         * @return
         */
        private static Map<Status, Counter> createStatusCounters(String resourceName) {
            Map<Status, Counter> result = new EnumMap<>(Status.class);

            for (Status status : Status.values()) {
                BasicCounter counter = new BasicCounter(

                    MonitorConfig.builder(EurekaClientNames.METRIC_TRANSPORT_PREFIX + "request")  // name : "eurekaClient.transport.request"
                        .withTag("id", resourceName)  // 枚举名称, id : "Register" 比如
                        .withTag("class", MetricsCollectingEurekaHttpClient.class.getSimpleName()) // class : "MetricsCollectingEurekaHttpClient"
                        .withTag("status", status.name())  // 状态码 ，"x100"
                        .build()
                );
                // JmxMonitorRegistry 注册 Monitor
                ServoUtil.register(counter);

                result.put(status, counter);
            }

            return result;
        }
    }
}
