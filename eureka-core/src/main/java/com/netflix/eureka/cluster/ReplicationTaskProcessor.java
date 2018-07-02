package com.netflix.eureka.cluster;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import com.netflix.eureka.cluster.protocol.ReplicationInstance;
import com.netflix.eureka.cluster.protocol.ReplicationInstance.ReplicationInstanceBuilder;
import com.netflix.eureka.cluster.protocol.ReplicationInstanceResponse;
import com.netflix.eureka.cluster.protocol.ReplicationList;
import com.netflix.eureka.cluster.protocol.ReplicationListResponse;
import com.netflix.eureka.util.batcher.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.netflix.eureka.cluster.protocol.ReplicationInstance.ReplicationInstanceBuilder.aReplicationInstance;

/**
 * 同步操作任务处理器
 *
 * @author Tomasz Bak
 */
class ReplicationTaskProcessor implements TaskProcessor<ReplicationTask> {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationTaskProcessor.class);

    private final HttpReplicationClient replicationClient;

    private final String peerId;

    private volatile long lastNetworkErrorTime;

    ReplicationTaskProcessor(String peerId, HttpReplicationClient replicationClient) {
        this.replicationClient = replicationClient;
        this.peerId = peerId;
    }

    @Override
    public ProcessingResult process(ReplicationTask task) {
        try {
            EurekaHttpResponse<?> httpResponse = task.execute();
            int statusCode = httpResponse.getStatusCode();
            Object entity = httpResponse.getEntity();
            if (logger.isDebugEnabled()) {
                logger.debug("Replication task {} completed with status {}, (includes entity {})", task.getTaskName(), statusCode, entity != null);
            }
            if (isSuccess(statusCode)) {
                task.handleSuccess();
            } else if (statusCode == 503) {
                logger.debug("Server busy (503) reply for task {}", task.getTaskName());
                return ProcessingResult.Congestion;
            } else {
                task.handleFailure(statusCode, entity);
                return ProcessingResult.PermanentError;
            }
        } catch (Throwable e) {
            if (isNetworkConnectException(e)) {
                logNetworkErrorSample(task, e);
                return ProcessingResult.TransientError;
            } else {
                logger.error(peerId + ": " + task.getTaskName() + "Not re-trying this exception because it does not seem to be a network exception", e);
                return ProcessingResult.PermanentError;
            }
        }
        return ProcessingResult.Success;
    }

    @Override
    public ProcessingResult process(List<ReplicationTask> tasks) {
        // 创建 批量提交同步操作任务的请求对象
        ReplicationList list = createReplicationListOf(tasks);
        try {
            // 发起 批量提交同步操作任务的请求
            EurekaHttpResponse<ReplicationListResponse> response = replicationClient.submitBatchUpdates(list);
            // 处理 批量提交同步操作任务的响应
            int statusCode = response.getStatusCode();
            if (!isSuccess(statusCode)) {
                if (statusCode == 503) {
                    logger.warn("Server busy (503) HTTP status code received from the peer {}; rescheduling tasks after delay", peerId);
                    return ProcessingResult.Congestion;
                } else {
                    // Unexpected error returned from the server. This should ideally never happen.
                    logger.error("Batch update failure with HTTP status code {}; discarding {} replication tasks", statusCode, tasks.size());
                    return ProcessingResult.PermanentError;
                }
            } else {
                handleBatchResponse(tasks, response.getEntity().getResponseList());
            }
        } catch (Throwable e) {
            if (isNetworkConnectException(e)) {
                logNetworkErrorSample(null, e);
                return ProcessingResult.TransientError;
            } else {
                logger.error("Not re-trying this exception because it does not seem to be a network exception", e);
                return ProcessingResult.PermanentError;
            }
        }
        return ProcessingResult.Success;
    }

    /**
     * We want to retry eagerly, but without flooding log file with tons of error entries.
     * As tasks are executed by a pool of threads the error logging multiplies. For example:
     * 20 threads * 100ms delay == 200 error entries / sec worst case
     * Still we would like to see the exception samples, so we print samples at regular intervals.
     */
    private void logNetworkErrorSample(ReplicationTask task, Throwable e) {
        long now = System.currentTimeMillis();
        if (now - lastNetworkErrorTime > 10000) { // 部分采样（采样条件，大于 10 s）
            lastNetworkErrorTime = now;
            StringBuilder sb = new StringBuilder();
            sb.append("Network level connection to peer ").append(peerId);
            if (task != null) {
                sb.append(" for task ").append(task.getTaskName());
            }
            sb.append("; retrying after delay");
            logger.error(sb.toString(), e);
        }
    }

    private void handleBatchResponse(List<ReplicationTask> tasks, List<ReplicationInstanceResponse> responseList) {
        if (tasks.size() != responseList.size()) {
            // This should ideally never happen unless there is a bug in the software.
            logger.error("Batch response size different from submitted task list ({} != {}); skipping response analysis", responseList.size(), tasks.size());
            return;
        }
        for (int i = 0; i < tasks.size(); i++) {
            handleBatchResponse(tasks.get(i), responseList.get(i));
        }
    }

    private void handleBatchResponse(ReplicationTask task, ReplicationInstanceResponse response) {
        // 执行成功
        int statusCode = response.getStatusCode();
        if (isSuccess(statusCode)) {
            task.handleSuccess();
            return;
        }

        // 执行失败
        try {
            task.handleFailure(response.getStatusCode(), response.getResponseEntity());
        } catch (Throwable e) {
            logger.error("Replication task " + task.getTaskName() + " error handler failure", e);
        }
    }

    /**
     * 将多个备份任务合并成一个备份任务集合
     *
     * @param tasks
     * @return
     */
    private ReplicationList createReplicationListOf(List<ReplicationTask> tasks) {
        ReplicationList list = new ReplicationList();
        for (ReplicationTask task : tasks) {
            // Only InstanceReplicationTask are batched.
            list.addReplicationInstance(createReplicationInstanceOf((InstanceReplicationTask)task));
        }
        return list;
    }

    /**
     * 响应码在200-300之间才是请求成功
     *
     * @param statusCode
     * @return
     */
    private static boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    /**
     * Check if the exception is some sort of network timeout exception (ie)
     * read,connect.
     *
     * @param e The exception for which the information needs to be found.
     * @return true, if it is a network timeout, false otherwise.
     */
    private static boolean isNetworkConnectException(Throwable e) {
        do {
            if (IOException.class.isInstance(e)) {
                return true;
            }
            e = e.getCause();
        } while (e != null);
        return false;
    }

    /**
     * 将InstanceInfo转换为待备份的信息
     * 当是注册动作时, 会发送InstanceInfo; 其他动作不需要
     *
     * @param task
     * @return
     */
    private static ReplicationInstance createReplicationInstanceOf(InstanceReplicationTask task) {
        ReplicationInstanceBuilder instanceBuilder = aReplicationInstance();
        // appName
        instanceBuilder.withAppName(task.getAppName());
        // id
        instanceBuilder.withId(task.getId());
        InstanceInfo instanceInfo = task.getInstanceInfo();
        if (instanceInfo != null) {
            // overrideStatus
            String overriddenStatus = task.getOverriddenStatus() == null ? null : task.getOverriddenStatus().name();
            instanceBuilder.withOverriddenStatus(overriddenStatus);
            // lastDirtyTimestamp
            instanceBuilder.withLastDirtyTimestamp(instanceInfo.getLastDirtyTimestamp());
            // instanceInfo; 当是注册动作时, 会发送InstanceInfo; 其他动作不需要
            if (task.shouldReplicateInstanceInfo()) {
                instanceBuilder.withInstanceInfo(instanceInfo);
            }
            // status
            String instanceStatus = instanceInfo.getStatus() == null ? null : instanceInfo.getStatus().name();
            instanceBuilder.withStatus(instanceStatus);
        }
        // action
        instanceBuilder.withAction(task.getAction());
        return instanceBuilder.build();
    }

}

