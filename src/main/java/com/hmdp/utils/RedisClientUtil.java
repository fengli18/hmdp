package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class RedisClientUtil {
    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public RedisClientUtil(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 往redis中存储，有过期时间
     * @param key
     * @param value
     * @param time
     * @param unit
     */
    public <T> void set(String key, T value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 逻辑过期（永不过期，得用户自己检查）
     * @param key
     * @param value
     * @param time
     * @param unit
     */
    public <T> void setWithLogicalExpire(String key, T value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData<T> redisData = new RedisData<>();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 通过互斥锁来解决缓存击穿问题（逻辑过期方式）
     * @param prefix
     * @param id
     * @param className
     * @param queryData
     * @param time
     * @param timeUnit
     * @return
     * @param <R>
     * @param <T>
     */
    public <R,T> R queryByLogicExpire(String prefix, T id, Class<R> className, Function<T, R> queryData, Long time, TimeUnit timeUnit) {
        // 从redis中查新商铺信息
        R shop = null;
        String shopKey = prefix+ id;
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        // 判断redis中是否存在该商铺
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        // 若redis存在 进行反序列化
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        shop = JSONUtil.toBean(data, className);
        // 获取过期时间
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            return shop;
        }
        // Redis中数据过期
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        try {
            // 判断是否获取锁
            boolean lock = getLock(lockKey);
            if (lock) {
                // 进行缓存重建
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    // 查询数据库
                    R apply = queryData.apply(id);
                    // 存入redis
                    this.setWithLogicalExpire(shopKey,apply,time,timeUnit);
                });
            }
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        } finally {
            // 不管结果如何 都得释放锁
            delLock(lockKey);
        }
        // 返回旧的商铺信息
        return shop;
    }

    /**
     * 解决缓存穿透问题 利用在redis中存空对象
     * @param prefix
     * @param id
     * @param className
     * @param queryData
     * @param time
     * @param timeUnit
     * @return
     * @param <R>
     * @param <T>
     */
    public <R, T> R queryWithRedisPass(
            String prefix, T id, Class<R> className, Function<T, R> queryData, Long time, TimeUnit timeUnit
    ) {
        // 从redis中查新商铺信息
        String shopKey = prefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        // 判断redis中是否存在该商铺
        if (StrUtil.isNotBlank(shopJson)) {
            // 存在 则进行反序列化操作
            R r = JSONUtil.toBean(shopJson, className);
            return r;
        }
        if ("".equals(shopJson)) {
            return null;
        }
        // Redis如果不存在这个商铺 则在数据库中查询
        R r = queryData.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(shopKey, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 如果数据库中存在 存入到redis
        String shopJson2 = JSONUtil.toJsonStr(r);
        this.set(shopKey, shopJson2, time, timeUnit);
        return r;
    }



    /**
     * 获取互斥锁
     * @param LockKey
     * @return
     */
    private boolean getLock(String LockKey) {
        Boolean aBoolean = stringRedisTemplate.opsForValue().setIfAbsent(LockKey, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(aBoolean);
    }

    /**
     * 删除互斥锁
     * @param lockKey
     */
    private void delLock(String lockKey) {
        stringRedisTemplate.delete(lockKey);
    }
}
