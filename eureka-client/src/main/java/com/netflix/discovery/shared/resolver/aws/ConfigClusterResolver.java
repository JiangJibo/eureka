package com.netflix.discovery.shared.resolver.aws;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.endpoint.EndpointUtils;
import com.netflix.discovery.shared.resolver.ClusterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A resolver that on-demand resolves from configuration what the endpoints should be.
 *
 * @author David Liu
 */
public class ConfigClusterResolver implements ClusterResolver<AwsEndpoint> {

    private static final Logger logger = LoggerFactory.getLogger(ConfigClusterResolver.class);

    private final EurekaClientConfig clientConfig;
    private final InstanceInfo myInstanceInfo;

    public ConfigClusterResolver(EurekaClientConfig clientConfig, InstanceInfo myInstanceInfo) {
        this.clientConfig = clientConfig;
        this.myInstanceInfo = myInstanceInfo;
    }

    @Override
    public String getRegion() {
        return clientConfig.getRegion();
    }

    @Override
    public List<AwsEndpoint> getClusterEndpoints() {
        // 使用 DNS 获取 EndPoint
        if (clientConfig.shouldUseDnsForFetchingServiceUrls()) {
            if (logger.isInfoEnabled()) {
                logger.info("Resolving eureka endpoints via DNS: {}", getDNSName());
            }
            return getClusterEndpointsFromDns();
        } else {
            // 直接配置实际访问地址
            logger.info("Resolving eureka endpoints via configuration");
            return getClusterEndpointsFromConfig();
        }
    }

    private List<AwsEndpoint> getClusterEndpointsFromDns() {
        String discoveryDnsName = getDNSName(); // 获取 集群根地址
        int port = Integer.parseInt(clientConfig.getEurekaServerPort()); // 端口

        // cheap enough so just re-use
        DnsTxtRecordClusterResolver dnsResolver = new DnsTxtRecordClusterResolver(
            getRegion(),
            discoveryDnsName,
            true, // 解析 zone
            port,
            false,
            clientConfig.getEurekaServerURLContext()
        );

        // 调用 DnsTxtRecordClusterResolver 解析 EndPoint
        List<AwsEndpoint> endpoints = dnsResolver.getClusterEndpoints();

        if (endpoints.isEmpty()) {
            logger.error("Cannot resolve to any endpoints for the given dnsName: {}", discoveryDnsName);
        }

        return endpoints;
    }

    /**
     * 根据配置来生成EurekaServer集群
     * 先获取"availability-zones" 里的当前region对应的zones, 然后获取第一个zone为myZone
     * 然后从serviceUrl里先获取myZone的urls,放进集合第一位;
     * 然后从myZone的顺序之后,依次获取zone对应的urls,存入集合
     * 然后返回 Map<Zone, List<ServiceUrl>> serviceUrls
     *
     * @return
     */
    private List<AwsEndpoint> getClusterEndpointsFromConfig() {
        // 获得 可用区,默认值 "defaultZone", 在获取zones时,只会获取到同一个Region下的zone, 也就是availZones的Region是同一个,都是当前应用的Region
        String[] availZones = clientConfig.getAvailabilityZones(clientConfig.getRegion());
        // 获取 应用实例自己 的 可用区, 当有多个可用区是返回第一个; 若未配置返回 default
        String myZone = InstanceInfo.getZone(availZones, myInstanceInfo);
        // 获得 可用区与 serviceUrls 的映射, 返回的结果集是一个LinkedHashMap
        Map<String, List<String>> serviceUrls = EndpointUtils.getServiceUrlsMapFromConfig(clientConfig, myZone, clientConfig.shouldPreferSameZoneEureka());
        // 拼装 EndPoint 集群结果, 相同zone的位于List前端
        List<AwsEndpoint> endpoints = new ArrayList<>();
        for (String zone : serviceUrls.keySet()) {
            for (String url : serviceUrls.get(zone)) {
                try {
                    //实例化亚马逊云节点, 也就是Eureka Server 节点
                    endpoints.add(new AwsEndpoint(url, getRegion(), zone));
                } catch (Exception ignore) {
                    logger.warn("Invalid eureka server URI: {}; removing from the server pool", url);
                }
            }
        }

        // 打印日志，EndPoint 集群
        if (logger.isDebugEnabled()) {
            logger.debug("Config resolved to {}", endpoints);
        }
        // 打印日志，解析结果为空
        if (endpoints.isEmpty()) {
            logger.error("Cannot resolve to any endpoints from provided configuration: {}", serviceUrls);
        }

        return endpoints;
    }

    private String getDNSName() {
        return "txt." + getRegion() + '.' + clientConfig.getEurekaServerDNSName();
    }
}
