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

package com.netflix.discovery.shared.resolver.aws;

import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.shared.resolver.ClusterResolver;
import com.netflix.discovery.shared.resolver.ResolverUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * 亲和Zone的集群解析器
 * 这是一个集群解析器,它会对Server List 重排序, 将那些zone与应用自身的zone相同的Server排在List前面
 * It is a cluster resolver that reorders the server list, such that the first server on the list
 * is in the same zone as the client. The server is chosen randomly from the available pool of server in
 * that zone. The remaining servers are appended in a random order, local zone first, followed by servers from other zones.
 *
 * @author Tomasz Bak
 */
public class ZoneAffinityClusterResolver implements ClusterResolver<AwsEndpoint> {

    private static final Logger logger = LoggerFactory.getLogger(ZoneAffinityClusterResolver.class);

    /**
     * 委托的解析器
     * 目前代码里为 {@link ConfigClusterResolver}
     */
    private final ClusterResolver<AwsEndpoint> delegate;
    /**
     * 应用实例的可用区
     * {@link EurekaClientConfig#getAvailabilityZones(String)}得到的字符串, 按","拆分后的第一个zone就作为myZone
     */
    private final String myZone;
    /**
     * 是否可用区亲和
     */
    private final boolean zoneAffinity;

    /**
     * A zoneAffinity defines zone affinity (true) or anti-affinity rules (false).
     */
    public ZoneAffinityClusterResolver(ClusterResolver<AwsEndpoint> delegate, String myZone, boolean zoneAffinity) {
        this.delegate = delegate;
        this.myZone = myZone;
        this.zoneAffinity = zoneAffinity;
    }

    @Override
    public String getRegion() {
        return delegate.getRegion();
    }

    /**
     * @return
     */
    @Override
    public List<AwsEndpoint> getClusterEndpoints() {
        // 拆分成 本地的可用区和非本地的可用区的 EndPoint 集群
        List<AwsEndpoint>[] parts = ResolverUtils.splitByZone(delegate.getClusterEndpoints(), myZone);
        List<AwsEndpoint> myZoneEndpoints = parts[0];   // myZone对应的可用区, 也就是 availabilityZones 里的第一个zone
        List<AwsEndpoint> remainingEndpoints = parts[1];  // 非myZone对应的可用区
        // 随机打乱 EndPoint 集群并进行合并
        List<AwsEndpoint> randomizedList = randomizeAndMerge(myZoneEndpoints, remainingEndpoints);
        // 非可用区亲和，将非本地的可用区的 EndPoint 集群放在前面
        if (!zoneAffinity) {
            Collections.reverse(randomizedList);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Local zone={}; resolved to: {}", myZone, randomizedList);
        }

        return randomizedList;
    }

    /**
     * 打乱AwsEndpoint顺序
     * 同一个EurekaClient每次打乱得到的顺序都是一样的,不同的Client得到的顺序不同
     * 效果：多个主机，实现对同一个 EndPoint 集群负载均衡的效果。单个主机，同一个 EndPoint 集群按照固定顺序访问
     *
     * @param myZoneEndpoints
     * @param remainingEndpoints
     * @return
     */
    private static List<AwsEndpoint> randomizeAndMerge(List<AwsEndpoint> myZoneEndpoints, List<AwsEndpoint> remainingEndpoints) {
        if (myZoneEndpoints.isEmpty()) {
            return ResolverUtils.randomize(remainingEndpoints); // 打乱
        }
        if (remainingEndpoints.isEmpty()) {
            return ResolverUtils.randomize(myZoneEndpoints); // 打乱
        }
        List<AwsEndpoint> mergedList = ResolverUtils.randomize(myZoneEndpoints); // 打乱
        // 将非本地可用区放在本地可用区之后
        mergedList.addAll(ResolverUtils.randomize(remainingEndpoints)); // 打乱
        return mergedList;
    }
}
