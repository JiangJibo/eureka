package com.netflix.discovery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A task for updating and replicating the local instanceinfo to the remote server. Properties of this task are:
 * - configured with a single update thread to guarantee sequential update to the remote server
 * - update tasks can be scheduled on-demand via onDemandUpdate()
 * - task processing is rate limited by burstSize
 * - a new update task is always scheduled automatically after an earlier update task. However if an on-demand task
 * is started, the scheduled automatic update task is discarded (and a new one will be scheduled after the new
 * on-demand update).
 *
 * 应用实例信息复制器, 将应用配置复制到{@link InstanceInfo}
 * 如果应用配置信息由改变,更新InstanceInfo,然后设置{@link InstanceInfo#isDirty()}, 之后重新向Server注册
 *
 * @author dliu
 */
class InstanceInfoReplicator implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(InstanceInfoReplicator.class);

    private final DiscoveryClient discoveryClient;
    /**
     * 应用实例信息
     */
    private final InstanceInfo instanceInfo;
    /**
     * 定时执行频率，单位：秒  30
     */
    private final int replicationIntervalSeconds;
    /**
     * 定时执行器
     */
    private final ScheduledExecutorService scheduler;
    /**
     * 定时执行任务的 Future
     */
    private final AtomicReference<Future> scheduledPeriodicRef;
    /**
     * 是否开启调度
     */
    private final AtomicBoolean started;

    /**
     * RateLimiter
     */
    private final RateLimiter rateLimiter;
    /**
     * 令牌桶上限，默认：2
     */
    private final int burstSize;
    /**
     * 令牌再装平均速率，默认：60 * 2 / 30 = 4
     */
    private final int allowedRatePerMinute;

    /**
     * @param discoveryClient
     * @param instanceInfo
     * @param replicationIntervalSeconds 备份间隔 30S
     * @param burstSize
     */
    InstanceInfoReplicator(DiscoveryClient discoveryClient, InstanceInfo instanceInfo, int replicationIntervalSeconds, int burstSize) {
        this.discoveryClient = discoveryClient;
        this.instanceInfo = instanceInfo;
        this.scheduler = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder()
                .setNameFormat("DiscoveryClient-InstanceInfoReplicator-%d")
                .setDaemon(true)
                .build());

        this.scheduledPeriodicRef = new AtomicReference<Future>();

        this.started = new AtomicBoolean(false);
        this.rateLimiter = new RateLimiter(TimeUnit.MINUTES);
        this.replicationIntervalSeconds = replicationIntervalSeconds;  // 30S
        this.burstSize = burstSize;

        this.allowedRatePerMinute = 60 * this.burstSize / this.replicationIntervalSeconds;
        logger.info("InstanceInfoReplicator onDemand update allowed rate per min is {}", allowedRatePerMinute);
    }

    /**
     * 40S后执行应用备份
     *
     * @param initialDelayMs
     */
    public void start(int initialDelayMs) {
        if (started.compareAndSet(false, true)) {
            // 设置脏状态
            instanceInfo.setIsDirty();  // for initial register
            // 提交任务，并设置该任务的 Future
            Future next = scheduler.schedule(this, initialDelayMs, TimeUnit.SECONDS);
            scheduledPeriodicRef.set(next);
        }
    }

    public void stop() {
        scheduler.shutdownNow();
        started.set(false);
    }

    /**
     * 在后台更新, 当应用状态发生改变时触发
     *
     * @return
     */
    public boolean onDemandUpdate() {
        if (rateLimiter.acquire(burstSize, allowedRatePerMinute)) { // 限流相关，跳过
            scheduler.submit(new Runnable() {
                @Override
                public void run() {
                    logger.debug("Executing on-demand update of local InstanceInfo");
                    // 取消任务
                    Future latestPeriodic = scheduledPeriodicRef.get();
                    if (latestPeriodic != null && !latestPeriodic.isDone()) {
                        logger.debug("Canceling the latest scheduled update, it will be rescheduled at the end of on demand update");
                        latestPeriodic.cancel(false);
                    }
                    // 再次调用
                    InstanceInfoReplicator.this.run();
                }
            });
            return true;
        } else {
            logger.warn("Ignoring onDemand update due to rate limiter");
            return false;
        }
    }

    /**
     * 如果应用配置发生改变，最长需要30S才会将配置信息更新至InstanceInfo, 然后同步至Eureka Server
     */
    @Override
    public void run() {
        try {
            // 刷新 应用实例信息, 包括hostName,ip,续约频率，续约过期时间,InstanceInfo状态等;
            // 当他们改变时，更新InstanceInfo里的相应数据, 同时设置 isInstanceInfoDirty = true
            discoveryClient.refreshInstanceInfo();
            // 判断 应用实例信息 是否脏了
            Long dirtyTimestamp = instanceInfo.isDirtyWithTime();
            // 如果数据不一致(脏了), 那么重新发起注册, 同步最新数据至Server
            if (dirtyTimestamp != null) {
                // 发起注册
                discoveryClient.register();
                // 如果注册期间数据未改变,那么还原为 非脏 状态
                instanceInfo.unsetIsDirty(dirtyTimestamp);
            }
        } catch (Throwable t) {
            logger.warn("There was a problem with the instance info replicator", t);
        } finally {
            // 设置下次备份任务, 指定延迟(30S)后再次执行
            Future next = scheduler.schedule(this, replicationIntervalSeconds, TimeUnit.SECONDS);
            scheduledPeriodicRef.set(next);
        }
    }

}