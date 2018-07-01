/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.netflix.eureka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class for getting a count in last X milliseconds.
 * 当前这个类仅仅用来计算续约次数
 * 每隔一分钟更新续约此时, 根据当前的续约次数和上一分钟的续约次数来判断一分钟内的续约次数是否低于阈值
 *
 * @author Karthik Ranganathan,Greg Kim
 */
public class MeasuredRate {

    private static final Logger logger = LoggerFactory.getLogger(MeasuredRate.class);
    /**
     * 上一分钟内续约次数
     */
    private final AtomicLong lastBucket = new AtomicLong(0);
    /**
     * 当前这分钟内续约次数
     */
    private final AtomicLong currentBucket = new AtomicLong(0);
    /**
     * 间隔, 默认60S
     */
    private final long sampleInterval;
    /**
     * 定时器
     */
    private final Timer timer;

    private volatile boolean isActive;

    /**
     * @param sampleInterval in milliseconds
     */
    public MeasuredRate(long sampleInterval) {
        this.sampleInterval = sampleInterval;
        this.timer = new Timer("Eureka-MeasureRateTimer", true);
        this.isActive = false;
    }

    public synchronized void start() {
        if (!isActive) {
            // 每隔一分钟执行一次定时任务, 更新最新的总的续约次数, 这样就能计算一分钟内续约的次数, 以此来判断续约次数是否低于阈值
            timer.schedule(new TimerTask() {

                @Override
                public void run() {
                    try {
                        // Zero out the current bucket. 将 currentBucket 赋值给lastBucket, 然后重置 currentBucket
                        lastBucket.set(currentBucket.getAndSet(0));
                    } catch (Throwable e) {
                        logger.error("Cannot reset the Measured Rate", e);
                    }
                }
            }, sampleInterval, sampleInterval);

            isActive = true;
        }
    }

    public synchronized void stop() {
        if (isActive) {
            timer.cancel();
            isActive = false;
        }
    }

    /**
     * Returns the count in the last sample interval.
     */
    public long getCount() {
        return lastBucket.get();
    }

    /**
     * Increments the count in the current sample interval.
     */
    public void increment() {
        currentBucket.incrementAndGet();
    }
}
