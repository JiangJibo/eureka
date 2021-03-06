package com.netflix.eureka.util.batcher;

import com.netflix.eureka.util.batcher.TaskExecutors.BatchWorkerRunnable;
import com.netflix.eureka.util.batcher.TaskExecutors.SingleTaskWorkerRunnable;
import com.netflix.eureka.util.batcher.TaskProcessor.ProcessingResult;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.stats.StatsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.netflix.eureka.Names.METRIC_REPLICATION_PREFIX;

/**
 * An active object with an internal thread accepting tasks from clients, and dispatching them to
 * workers in a pull based manner. Workers explicitly request an item or a batch of items whenever they are
 * available. This guarantees that data to be processed are always up to date, and no stale data processing is done.
 *
 * <h3>Task identification</h3>
 * Each task passed for processing has a corresponding task id. This id is used to remove duplicates (replace
 * older copies with newer ones).
 *
 * <h3>Re-processing</h3>
 * If data processing by a worker failed, and the failure is transient in nature, the worker will put back the
 * task(s) back to the {@link AcceptorExecutor}. This data will be merged with current workload, possibly discarded if
 * a newer version has been already received.
 *
 * 任务接收执行器
 *
 * @author Tomasz Bak
 */
class AcceptorExecutor<ID, T> {

    private static final Logger logger = LoggerFactory.getLogger(AcceptorExecutor.class);

    /**
     * 待执行队列最大数量
     * {@link #processingOrder}
     */
    private final int maxBufferSize;
    /**
     * 单个批量任务包含任务最大数量
     */
    private final int maxBatchingSize;
    /**
     * 批量任务等待最大延迟时长，单位：毫秒
     */
    private final long maxBatchingDelay;

    /**
     * 是否关闭
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    /**
     * 接收任务队列
     */
    private final BlockingQueue<TaskHolder<ID, T>> acceptorQueue = new LinkedBlockingQueue<>();
    /**
     * 重新执行任务队列
     */
    private final BlockingDeque<TaskHolder<ID, T>> reprocessQueue = new LinkedBlockingDeque<>();
    /**
     * 接收任务线程
     */
    private final Thread acceptorThread;

    /**
     * 待执行任务映射
     */
    private final Map<ID, TaskHolder<ID, T>> pendingTasks = new HashMap<>();
    /**
     * 待执行队列的ID顺序
     */
    private final Deque<ID> processingOrder = new LinkedList<>();

    /**
     * 单任务工作请求信号量
     */
    private final Semaphore singleItemWorkRequests = new Semaphore(0);
    /**
     * 单任务工作队列
     */
    private final BlockingQueue<TaskHolder<ID, T>> singleItemWorkQueue = new LinkedBlockingQueue<>();

    /**
     * 批量任务工作请求信号量
     */
    private final Semaphore batchWorkRequests = new Semaphore(0);
    /**
     * 批量任务工作队列
     */
    private final BlockingQueue<List<TaskHolder<ID, T>>> batchWorkQueue = new LinkedBlockingQueue<>();

    /**
     * 网络通信整形器
     */
    private final TrafficShaper trafficShaper;

    /*
     * Metrics
     */
    @Monitor(name = METRIC_REPLICATION_PREFIX + "acceptedTasks", description = "Number of accepted tasks", type = DataSourceType.COUNTER)
    volatile long acceptedTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "replayedTasks", description = "Number of replayedTasks tasks", type = DataSourceType.COUNTER)
    volatile long replayedTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "expiredTasks", description = "Number of expired tasks", type = DataSourceType.COUNTER)
    volatile long expiredTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "overriddenTasks", description = "Number of overridden tasks", type = DataSourceType.COUNTER)
    volatile long overriddenTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "queueOverflows", description = "Number of queue overflows", type = DataSourceType.COUNTER)
    volatile long queueOverflows;

    private final Timer batchSizeMetric;

    /**
     * @param id
     * @param maxBufferSize          待执行队列最大数量  10000
     * @param maxBatchingSize        单个批量任务包含任务最大数量  250
     * @param maxBatchingDelay       批量任务等待最大延迟时长，单位：毫秒  500
     * @param congestionRetryDelayMs 请求限流延迟重试时间，单位：毫秒  1000
     * @param networkFailureRetryMs  网络失败延迟重试时长，单位：毫秒 100
     */
    AcceptorExecutor(String id,
                     int maxBufferSize,
                     int maxBatchingSize,
                     long maxBatchingDelay,
                     long congestionRetryDelayMs,
                     long networkFailureRetryMs) {
        this.maxBufferSize = maxBufferSize;
        this.maxBatchingSize = maxBatchingSize;
        this.maxBatchingDelay = maxBatchingDelay;

        // 创建 网络通信整形器
        this.trafficShaper = new TrafficShaper(congestionRetryDelayMs, networkFailureRetryMs);

        // 创建 接收任务线程, 同时启动处理线程, 也就是"AcceptorRunner"对象
        ThreadGroup threadGroup = new ThreadGroup("eurekaTaskExecutors");
        this.acceptorThread = new Thread(threadGroup, new AcceptorRunner(), "TaskAcceptor-" + id);
        this.acceptorThread.setDaemon(true);
        this.acceptorThread.start();

        // TODO 芋艿：监控相关，暂时无视
        final double[] percentiles = {50.0, 95.0, 99.0, 99.5};
        final StatsConfig statsConfig = new StatsConfig.Builder()
            .withSampleSize(1000)
            .withPercentiles(percentiles)
            .withPublishStdDev(true)
            .build();
        final MonitorConfig config = MonitorConfig.builder(METRIC_REPLICATION_PREFIX + "batchSize").build();
        this.batchSizeMetric = new StatsTimer(config, statsConfig);
        try {
            Monitors.registerObject(id, this);
        } catch (Throwable e) {
            logger.warn("Cannot register servo monitor for this object", e);
        }
    }

    /**
     * 将一个任务添加任务接收队列
     *
     * @param id
     * @param task
     * @param expiryTime
     */
    void process(ID id, T task, long expiryTime) {
        acceptorQueue.add(new TaskHolder<ID, T>(id, task, expiryTime));
        acceptedTasks++;
    }

    void reprocess(List<TaskHolder<ID, T>> holders, ProcessingResult processingResult) {
        // 添加到 重新执行队列
        reprocessQueue.addAll(holders);

        // TODO 芋艿：监控相关，暂时无视
        replayedTasks += holders.size();

        // 提交任务结果给 TrafficShaper
        trafficShaper.registerFailure(processingResult);
    }

    void reprocess(TaskHolder<ID, T> taskHolder, ProcessingResult processingResult) {
        // 添加到 重新执行队列
        reprocessQueue.add(taskHolder);

        // TODO 芋艿：监控相关，暂时无视
        replayedTasks++;

        // 提交任务结果给 TrafficShaper
        trafficShaper.registerFailure(processingResult);
    }

    /**
     * 当是{@link SingleTaskWorkerRunnable} 时, 调用此方法, 开启单个任务的调度
     *
     * @return
     */
    BlockingQueue<TaskHolder<ID, T>> requestWorkItem() {
        // 将信号量的值增加为1
        singleItemWorkRequests.release();
        return singleItemWorkQueue;
    }

    /**
     * 当是{@link BatchWorkerRunnable#getWork()} 时, 调用此方法, 开启单个任务的调度
     *
     * @return
     */
    BlockingQueue<List<TaskHolder<ID, T>>> requestWorkItems() {
        // 将信号量的值增加为1
        batchWorkRequests.release();
        return batchWorkQueue;
    }

    void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            acceptorThread.interrupt();
        }
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "acceptorQueueSize", description = "Number of tasks waiting in the acceptor queue", type = DataSourceType.GAUGE)
    public long getAcceptorQueueSize() {
        return acceptorQueue.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "reprocessQueueSize", description = "Number of tasks waiting in the reprocess queue",
        type = DataSourceType.GAUGE)
    public long getReprocessQueueSize() {
        return reprocessQueue.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "queueSize", description = "Task queue size", type = DataSourceType.GAUGE)
    public long getQueueSize() {
        return pendingTasks.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "pendingJobRequests", description = "Number of worker threads awaiting job assignment",
        type = DataSourceType.GAUGE)
    public long getPendingJobRequests() {
        return singleItemWorkRequests.availablePermits() + batchWorkRequests.availablePermits();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "availableJobs", description = "Number of jobs ready to be taken by the workers", type = DataSourceType.GAUGE)
    public long workerTaskQueueSize() {
        return singleItemWorkQueue.size() + batchWorkQueue.size();
    }

    class AcceptorRunner implements Runnable {

        @Override
        public void run() {
            long scheduleTime = 0;
            while (!isShutdown.get()) {
                try {
                    // 处理完输入队列( 接收队列 + 重新执行队列 )
                    drainInputQueues();

                    // 待执行任务数量
                    int totalItems = processingOrder.size();

                    // 计算调度时间
                    long now = System.currentTimeMillis();
                    if (scheduleTime < now) {
                        scheduleTime = now + trafficShaper.transmissionDelay();
                    }

                    // 调度
                    if (scheduleTime <= now) {
                        // 调度批量任务, 将待处理任务合并成一个List<TaskHolder<ID, T>>, 加入 batchWorkQueue 中
                        assignBatchWork();
                        // 调度单任务 , 将一个待处理任务 TaskHolder<ID, T> 加入 singleItemWorkQueue 中
                        assignSingleItemWork();
                    }

                    // 1）任务执行器无任务请求，正在忙碌处理之前的任务；或者 2）任务延迟调度。睡眠 10 毫秒，避免资源浪费。
                    // If no worker is requesting data or there is a delay injected by the traffic shaper,
                    // sleep for some time to avoid tight loop.
                    if (totalItems == processingOrder.size()) {
                        Thread.sleep(10);
                    }
                } catch (InterruptedException ex) {
                    // Ignore
                } catch (Throwable e) {
                    // Safe-guard, so we never exit this loop in an uncontrolled way.
                    logger.warn("Discovery AcceptorThread error", e);
                }
            }
        }

        /**
         * 待处理任务是否超过批次限制, 250个
         *
         * @return
         */
        private boolean isFull() {
            return pendingTasks.size() >= maxBufferSize;
        }

        private void drainInputQueues() throws InterruptedException {
            do {
                // 处理完需要重新执行的任务队列
                drainReprocessQueue();
                // 处理完新接收的任务队列
                drainAcceptorQueue();

                if (!isShutdown.get()) {
                    // If all queues are empty, block for a while on the acceptor queue  所有队列为空，等待 10 ms，看接收队列是否有新任务
                    if (reprocessQueue.isEmpty() && acceptorQueue.isEmpty() && pendingTasks.isEmpty()) {
                        TaskHolder<ID, T> taskHolder = acceptorQueue.poll(10, TimeUnit.MILLISECONDS);
                        if (taskHolder != null) {
                            appendTaskHolder(taskHolder);
                        }
                    }
                }
            } while (!reprocessQueue.isEmpty() || !acceptorQueue.isEmpty() || pendingTasks.isEmpty()); // 处理完输入队列( 接收队列 + 重新执行队列 )
        }

        private void drainAcceptorQueue() {
            while (!acceptorQueue.isEmpty()) { // 循环，直到接收队列为空
                appendTaskHolder(acceptorQueue.poll());
            }
        }

        private void drainReprocessQueue() {
            long now = System.currentTimeMillis();
            // 当需要重新处理的队列不为空, 且当前批次还未满
            while (!reprocessQueue.isEmpty() && !isFull()) {
                TaskHolder<ID, T> taskHolder = reprocessQueue.pollLast(); // 优先拿较新的任务
                ID id = taskHolder.getId();
                if (taskHolder.getExpiryTime() <= now) { // 过期
                    expiredTasks++;
                } else if (pendingTasks.containsKey(id)) { // 已存在
                    overriddenTasks++;
                } else { // 添加到待执行队列
                    pendingTasks.put(id, taskHolder);
                    processingOrder.addFirst(id); // 提交到队头
                }
            }
            // 如果待执行队列已满，清空重新执行队列，放弃较早的任务
            if (isFull()) {
                queueOverflows += reprocessQueue.size();
                reprocessQueue.clear();
            }
        }

        private void appendTaskHolder(TaskHolder<ID, T> taskHolder) {
            // 如果待执行队列已满，移除待处理队列头部，放弃较早的任务(需重复执行的任务)
            if (isFull()) {
                pendingTasks.remove(processingOrder.poll());
                queueOverflows++;
            }
            // 添加到待执行队列
            TaskHolder<ID, T> previousTask = pendingTasks.put(taskHolder.getId(), taskHolder);
            if (previousTask == null) {
                processingOrder.add(taskHolder.getId());
            } else {
                overriddenTasks++;
            }
        }

        /**
         * {@link #requestWorkItem}开启信号量, 也就是请求一次任务，也就是上一次任务处理完了
         */
        void assignSingleItemWork() {
            if (!processingOrder.isEmpty()) { // 待执行任队列不为空
                // 获取 单任务工作请求信号量, 若未提前开启, 则获取不到
                if (singleItemWorkRequests.tryAcquire(1)) {
                    // 【循环】获取单任务
                    long now = System.currentTimeMillis();
                    while (!processingOrder.isEmpty()) {
                        ID id = processingOrder.poll();
                        TaskHolder<ID, T> holder = pendingTasks.remove(id);
                        if (holder.getExpiryTime() > now) {
                            singleItemWorkQueue.add(holder);
                            return;
                        }
                        expiredTasks++;
                    }
                    // 获取不到单任务，释放请求信号量
                    singleItemWorkRequests.release();
                }
            }
        }

        /**
         * 确定能够执行批量处理,将待处理任务合并成一个集合任务, List<TaskHolder<ID, T>>
         * {@link #requestWorkItems}开启信号量, 也就是请求一次任务，也就是上一次任务处理完了
         */
        void assignBatchWork() {
            // 一定要符合条件后才能触发批量操作
            if (hasEnoughTasksForNextBatch()) {
                // 获取 批量任务工作请求信号量, 若未提前开启, 则获取不到
                if (batchWorkRequests.tryAcquire(1)) {
                    // 获取批量任务
                    long now = System.currentTimeMillis();
                    //  每次处理的量不能超过最大限制
                    int len = Math.min(maxBatchingSize, processingOrder.size());
                    List<TaskHolder<ID, T>> holders = new ArrayList<>(len);
                    while (holders.size() < len && !processingOrder.isEmpty()) {
                        ID id = processingOrder.poll();
                        TaskHolder<ID, T> holder = pendingTasks.remove(id);
                        if (holder.getExpiryTime() > now) { // 未过期
                            holders.add(holder);
                        } else {
                            expiredTasks++;
                        }
                    }
                    //
                    if (holders.isEmpty()) { // 未调度到批量任务，释放请求信号量
                        batchWorkRequests.release();
                    } else { // 添加批量任务到批量任务工作队列
                        batchSizeMetric.record(holders.size(), TimeUnit.MILLISECONDS);
                        batchWorkQueue.add(holders);
                    }
                }
            }
        }

        private boolean hasEnoughTasksForNextBatch() {
            // 待执行队列为空
            if (processingOrder.isEmpty()) {
                return false;
            }
            // 待执行任务映射已满, 容量 >= 250
            if (pendingTasks.size() >= maxBufferSize) {
                return true;
            }

            // 到达批量任务处理最大等待延迟( 通过待处理队列的头部任务判断 ), 头部任务的生成时间距离现在已经超过了500ms
            TaskHolder<ID, T> nextHolder = pendingTasks.get(processingOrder.peek());
            long delay = System.currentTimeMillis() - nextHolder.getSubmitTimestamp();
            return delay >= maxBatchingDelay;
        }
    }
}
