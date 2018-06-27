package com.netflix.eureka.registry;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.discovery.shared.LookupService;
import com.netflix.discovery.shared.Pair;
import com.netflix.eureka.lease.LeaseManager;

import java.util.List;
import java.util.Map;

/**
 * 实例注册
 *
 * @author Tomasz Bak
 */
public interface InstanceRegistry extends LeaseManager<InstanceInfo>, LookupService<String> {

    /**
     * 开启运输
     *
     * @param applicationInfoManager
     * @param count
     */
    void openForTraffic(ApplicationInfoManager applicationInfoManager, int count);

    void shutdown();

    @Deprecated
    void storeOverriddenStatusIfRequired(String id, InstanceStatus overriddenStatus);

    /**
     * 当需要时存储覆盖状态
     *
     * @param appName
     * @param id
     * @param overriddenStatus
     */
    void storeOverriddenStatusIfRequired(String appName, String id, InstanceStatus overriddenStatus);

    /**
     * 状态更新
     *
     * @param appName
     * @param id
     * @param newStatus
     * @param lastDirtyTimestamp
     * @param isReplication
     * @return
     */
    boolean statusUpdate(String appName, String id, InstanceStatus newStatus,
                         String lastDirtyTimestamp, boolean isReplication);

    /**
     * 删除状态覆盖
     *
     * @param appName
     * @param id
     * @param newStatus
     * @param lastDirtyTimestamp
     * @param isReplication
     * @return
     */
    boolean deleteStatusOverride(String appName, String id, InstanceStatus newStatus,
                                 String lastDirtyTimestamp, boolean isReplication);

    Map<String, InstanceStatus> overriddenInstanceStatusesSnapshot();

    Applications getApplicationsFromLocalRegionOnly();

    List<Application> getSortedApplications();

    /**
     * Get application information.
     *
     * @param appName             The name of the application
     * @param includeRemoteRegion true, if we need to include applications from remote regions
     *                            as indicated by the region {@link java.net.URL} by this property
     *                            {@link com.netflix.eureka.EurekaServerConfig#getRemoteRegionUrls()}, false otherwise
     * @return the application
     */
    Application getApplication(String appName, boolean includeRemoteRegion);

    /**
     * Gets the {@link InstanceInfo} information.
     *
     * @param appName the application name for which the information is requested.
     * @param id      the unique identifier of the instance.
     * @return the information about the instance.
     */
    InstanceInfo getInstanceByAppAndId(String appName, String id);

    /**
     * Gets the {@link InstanceInfo} information.
     *
     * @param appName              the application name for which the information is requested.
     * @param id                   the unique identifier of the instance.
     * @param includeRemoteRegions true, if we need to include applications from remote regions
     *                             as indicated by the region {@link java.net.URL} by this property
     *                             {@link com.netflix.eureka.EurekaServerConfig#getRemoteRegionUrls()}, false otherwise
     * @return the information about the instance.
     */
    InstanceInfo getInstanceByAppAndId(String appName, String id, boolean includeRemoteRegions);

    /**
     * 清空注册表
     */
    void clearRegistry();

    /**
     * 初始化响应缓存
     */
    void initializedResponseCache();

    /**
     * 获取响应缓存
     *
     * @return
     */
    ResponseCache getResponseCache();

    /**
     * 获取最近一分钟内续约的实例个数
     *
     * @return
     */
    long getNumOfRenewsInLastMin();

    int getNumOfRenewsPerMinThreshold();

    int isBelowRenewThresold();

    List<Pair<Long, String>> getLastNRegisteredInstances();

    List<Pair<Long, String>> getLastNCanceledInstances();

    /**
     * Checks whether lease expiration is enabled.
     *
     * @return true if enabled
     */
    boolean isLeaseExpirationEnabled();

    boolean isSelfPreservationModeEnabled();

}
