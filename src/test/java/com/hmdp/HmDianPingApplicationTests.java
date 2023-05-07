package com.hmdp;

import com.hmdp.dto.UserDTO;
import com.hmdp.entity.test;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    private final ExecutorService service = Executors.newFixedThreadPool(500);

    @Test
    public void test() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300);
        Runnable runnable = () -> {
            for (int i = 0; i < 100; i++) {
                long order = redisIdWorker.nextId("order");
                System.out.println("id=" + order);
            }
            countDownLatch.countDown();
        };
        long Begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            service.submit(runnable);
        }
        countDownLatch.await();
        long End = System.currentTimeMillis();
        System.out.println("TIME:"+ (End - Begin));
    }

}
