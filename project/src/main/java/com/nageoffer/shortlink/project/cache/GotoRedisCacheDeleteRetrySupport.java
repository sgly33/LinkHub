/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.shortlink.project.cache;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_IS_NULL_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_SHORT_LINK_KEY;

/**
 * 跳转缓存 Redis 删除失败时加入本地队列，定时批量重试删除；成功后通知本地与其它实例失效跳转缓存。
 * 使用本地内存队列，每个节点独立处理自己失败的删除任务，避免依赖Redis队列和多实例重复处理问题。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GotoRedisCacheDeleteRetrySupport {

    private final StringRedisTemplate stringRedisTemplate;
    private final ApplicationEventPublisher eventPublisher;

    /**
     * 本地重试队列（每个节点独立维护）
     */
    private final ConcurrentLinkedQueue<RetryTask> localRetryQueue = new ConcurrentLinkedQueue<>();

    /**
     * 在定时任务中连续删除失败达到此次数后丢弃。
     */
    @Value("${short-link.cache.goto-delete-retry-max-attempts:3}")
    private int maxScheduledAttempts;

    /**
     * 尝试删除 Redis 中的跳转缓存 key；若调用异常则写入本地重试队列。
     *
     * @param redisKey Redis key
     * @return true 表示本次删除调用未抛异常（含 key 不存在）
     */
    public boolean deleteOrEnqueue(String redisKey) {
        try {
            stringRedisTemplate.delete(redisKey);
            return true;
        } catch (Exception ex) {
            log.warn("[goto-cache] Redis delete failed, will retry locally, key={}", redisKey, ex);
            localRetryQueue.offer(new RetryTask(redisKey, 0, System.currentTimeMillis()));
            return false;
        }
    }

    /**
     * 定时处理本地重试队列
     */
    @Scheduled(fixedDelayString = "${short-link.cache.goto-delete-retry-interval-ms:30000}")
    public void processLocalRetryQueue() {
        int processed = 0;
        int succeeded = 0;
        int abandoned = 0;

        Iterator<RetryTask> iterator = localRetryQueue.iterator();
        while (iterator.hasNext()) {
            RetryTask task = iterator.next();
            processed++;

            try {
                // 尝试删除 Redis key
                stringRedisTemplate.delete(task.redisKey);

                // 删除成功，发布事件通知本地和其他节点清理缓存
                String fullShortUrl = parseFullShortUrl(task.redisKey);
                if (fullShortUrl != null) {
                    eventPublisher.publishEvent(new GotoRedisCacheRetrySuccessEvent(fullShortUrl));
                }

                iterator.remove();
                succeeded++;
                log.info("[goto-cache] Retry delete succeeded: {}", task.redisKey);

            } catch (Exception ex) {
                task.failures++;

                if (task.failures >= maxScheduledAttempts) {
                    // 达到最大重试次数，放弃
                    iterator.remove();
                    abandoned++;
                    log.warn(
                            "[goto-cache] Retry delete abandoned after {} failures (max={}), key={}",
                            task.failures,
                            maxScheduledAttempts,
                            task.redisKey,
                            ex);
                } else {
                    log.debug(
                            "[goto-cache] Retry delete still failed ({}/{}), will retry again, key={}",
                            task.failures,
                            maxScheduledAttempts,
                            task.redisKey);
                }
            }
        }

        if (processed > 0) {
            log.info(
                    "[goto-cache] Retry queue processed: {} total, {} succeeded, {} abandoned, {} remaining in queue",
                    processed, succeeded, abandoned, localRetryQueue.size());
        }
    }

    /**
     * 从 Redis key 中解析出完整短链接
     */
    static String parseFullShortUrl(String redisKey) {
        String gotoPrefix = String.format(GOTO_SHORT_LINK_KEY, "");
        if (redisKey.startsWith(gotoPrefix)) {
            return redisKey.substring(gotoPrefix.length());
        }
        String isNullPrefix = String.format(GOTO_IS_NULL_SHORT_LINK_KEY, "");
        if (redisKey.startsWith(isNullPrefix)) {
            return redisKey.substring(isNullPrefix.length());
        }
        return null;
    }

    /**
     * 重试任务
     */
    @Data
    @AllArgsConstructor
    static class RetryTask {
        /**
         * Redis key
         */
        private String redisKey;

        /**
         * 已失败次数
         */
        private int failures;

        /**
         * 入队时间
         */
        private long enqueueTime;
    }
}