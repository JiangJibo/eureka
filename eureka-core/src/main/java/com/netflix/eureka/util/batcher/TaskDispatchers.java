package com.netflix.eureka.util.batcher;

/**
 * See {@link TaskDispatcher} for an overview.
 *
 * @author Tomasz Bak
 */
public class TaskDispatchers {

    /**
     * 创建单任务执行的分发器
     *
     * @param id                     任务执行器编号
     * @param maxBufferSize          待执行队列最大数量
     * @param workerCount            任务执行器工作线程数
     * @param maxBatchingDelay       批量任务等待最大延迟时长，单位：毫秒
     * @param congestionRetryDelayMs 请求限流延迟重试时间，单位：毫秒
     * @param networkFailureRetryMs  网络失败延迟重试时长，单位：毫秒
     * @param taskProcessor          任务处理器
     * @param <ID>                   任务编号泛型
     * @param <T>                    任务泛型
     * @return 单任务执行的分发器
     */
    public static <ID, T> TaskDispatcher<ID, T> createNonBatchingTaskDispatcher(String id,
                                                                                int maxBufferSize,
                                                                                int workerCount,
                                                                                long maxBatchingDelay,
                                                                                long congestionRetryDelayMs,
                                                                                long networkFailureRetryMs,
                                                                                TaskProcessor<T> taskProcessor) {
        // 创建 任务接收执行器
        final AcceptorExecutor<ID, T> acceptorExecutor = new AcceptorExecutor<>(
            id, maxBufferSize, /* workloadSize = 1 */1, maxBatchingDelay, congestionRetryDelayMs, networkFailureRetryMs
        );
        final TaskExecutors<ID, T> taskExecutor = TaskExecutors.singleItemExecutors(id, workerCount, taskProcessor, acceptorExecutor);
        return new TaskDispatcher<ID, T>() {
            @Override
            public void process(ID id, T task, long expiryTime) {
                acceptorExecutor.process(id, task, expiryTime);
            }

            @Override
            public void shutdown() {
                acceptorExecutor.shutdown();
                taskExecutor.shutdown();
            }
        };
    }

    /**
     * 创建批量任务执行的分发器
     *
     * @param id                     任务执行器编号
     * @param maxBufferSize          待执行队列最大数量  10000
     * @param workloadSize           单个批量任务包含任务最大数量  250
     * @param workerCount            任务执行器工作线程数  20
     * @param maxBatchingDelay       批量任务等待最大延迟时长，单位：毫秒  500
     * @param congestionRetryDelayMs 请求限流延迟重试时间，单位：毫秒  1000
     * @param networkFailureRetryMs  网络失败延迟重试时长，单位：毫秒 100
     * @param taskProcessor          任务处理器
     * @param <ID>                   任务编号泛型
     * @param <T>                    任务泛型
     * @return 批量任务执行的分发器
     */
    public static <ID, T> TaskDispatcher<ID, T> createBatchingTaskDispatcher(String id,
                                                                             int maxBufferSize,
                                                                             int workloadSize,
                                                                             int workerCount,
                                                                             long maxBatchingDelay,
                                                                             long congestionRetryDelayMs,
                                                                             long networkFailureRetryMs,
                                                                             TaskProcessor<T> taskProcessor) {
        // 创建 任务接收执行器, 其仅负责接受任务,不负责处理
        final AcceptorExecutor<ID, T> acceptorExecutor = new AcceptorExecutor<>(
            id, maxBufferSize, workloadSize, maxBatchingDelay, congestionRetryDelayMs, networkFailureRetryMs
        );
        // 创建 批量任务执行器, 从 acceptorExecutor 处拉取任务执行。在实例化时就启动了工作线程
        final TaskExecutors<ID, T> taskExecutor = TaskExecutors.batchExecutors(id, workerCount, taskProcessor, acceptorExecutor);
        // 创建 批量任务分发器
        return new TaskDispatcher<ID, T>() {
            @Override
            public void process(ID id, T task, long expiryTime) {
                // 将任务放入接受队列中, 是一个阻塞队列, taskExecutor会不断的从此队列获取任务然后执行
                acceptorExecutor.process(id, task, expiryTime);
            }

            @Override
            public void shutdown() {
                acceptorExecutor.shutdown();
                taskExecutor.shutdown();
            }
        };
    }
}
