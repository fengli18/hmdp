package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private final long BEGIN_TIMESTAMP = 1640995200L;

    /**
     * 设置id自增长
     * @param prefix
     * @return
     */
    public long nextId(String prefix) {
        // 获取当前的时间撮
        LocalDateTime now = LocalDateTime.now();
        long NOW_TIMESTAMP = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = NOW_TIMESTAMP - BEGIN_TIMESTAMP;
        // 设置当前日期格式
        String nowTimeFormat = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 设置自增长的key 获取自增长的值
        String icrKey = RedisConstants.ID_INCRMENT_KEY + prefix + nowTimeFormat;
        long increment = stringRedisTemplate.opsForValue().increment(icrKey);

        return timeStamp << 32 | increment;
    }
}
