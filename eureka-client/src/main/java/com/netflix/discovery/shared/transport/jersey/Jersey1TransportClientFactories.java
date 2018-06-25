package com.netflix.discovery.shared.transport.jersey;

import com.netflix.appinfo.EurekaClientIdentity;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.TransportClientFactory;
import com.netflix.discovery.shared.transport.decorator.MetricsCollectingEurekaHttpClient;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.client.apache4.ApacheHttpClient4;

import java.util.Collection;

public class Jersey1TransportClientFactories implements TransportClientFactories<ClientFilter> {

    @Override
    @Deprecated
    public TransportClientFactory newTransportClientFactory(final Collection<ClientFilter> additionalFilters,
                                                            final EurekaJerseyClient providedJerseyClient) {
        ApacheHttpClient4 apacheHttpClient = providedJerseyClient.getClient();
        if (additionalFilters != null) {
            for (ClientFilter filter : additionalFilters) {
                if (filter != null) {
                    apacheHttpClient.addFilter(filter);
                }
            }
        }

        final TransportClientFactory jerseyFactory = new JerseyEurekaHttpClientFactory(providedJerseyClient, false);
        final TransportClientFactory metricsFactory = MetricsCollectingEurekaHttpClient.createFactory(jerseyFactory);

        return new TransportClientFactory() {
            @Override
            public EurekaHttpClient newClient(EurekaEndpoint serviceUrl) {
                return metricsFactory.newClient(serviceUrl);
            }

            @Override
            public void shutdown() {
                metricsFactory.shutdown();
                jerseyFactory.shutdown();
            }
        };
    }

    /**
     * 创建Jersey1的传输客户端工厂
     *
     * @param clientConfig      客户端配置
     * @param additionalFilters 额外的客户端过滤器
     * @param myInstanceInfo    应用实例信息
     * @return
     */
    @Override
    public TransportClientFactory newTransportClientFactory(final EurekaClientConfig clientConfig,
                                                            final Collection<ClientFilter> additionalFilters,
                                                            final InstanceInfo myInstanceInfo) {
        // JerseyEurekaHttpClientFactory
        final TransportClientFactory jerseyFactory = JerseyEurekaHttpClientFactory.create(
            clientConfig,
            additionalFilters,
            myInstanceInfo,
            new EurekaClientIdentity(myInstanceInfo.getIPAddr())
        );

        // TransportClientFactory ,  Metrics： ['metrɪks] 指标, 指标收集Eureka Http Client, 延迟,连接错误等指标
        final TransportClientFactory metricsFactory = MetricsCollectingEurekaHttpClient.createFactory(jerseyFactory); // 委托 TransportClientFactory

        return new TransportClientFactory() {
            @Override
            public EurekaHttpClient newClient(EurekaEndpoint serviceUrl) {
                return metricsFactory.newClient(serviceUrl);
            }

            @Override
            public void shutdown() {
                metricsFactory.shutdown();
                jerseyFactory.shutdown();
            }
        };
    }

}