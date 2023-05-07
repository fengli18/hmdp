package com.hmdp.utils.lockImpl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisLock;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class RedisLockImpl implements RedisLock {

    private StringRedisTemplate stringRedisTemplate;
    private String name;

    public RedisLockImpl(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    @Override
    public boolean getLock(long timeout) {
        // 获取锁的key和值
        long id = Thread.currentThread().getId();
        String lockKey = RedisConstants.LOCK_KEY + name;
        String threadId = RedisConstants.THREAD_ID_KEY + id;
        // 设置锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, threadId, timeout, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success);
    }

    @Override
    public void delLock() {
        // 释放锁
        String lockKey = RedisConstants.LOCK_KEY + name;
        String threadId = RedisConstants.THREAD_ID_KEY + Thread.currentThread().getId();
        String threadId2 = stringRedisTemplate.opsForValue().get(lockKey);
        // 看锁是否属于当前线程 是的话才会释放锁
        if (threadId.equals(threadId2)) {
            stringRedisTemplate.delete(lockKey);
        }
    }
}
