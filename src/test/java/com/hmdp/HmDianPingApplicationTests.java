package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

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
        System.out.println("TIME:" + (End - Begin));
    }

    @Test
    void test2() {
        // 获取店铺信息
        List<Shop> list = shopService.list();
        // 让店铺根据type_id进行分组
        Map<Long, List<Shop>> collect = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        for (Map.Entry<Long, List<Shop>> shopGroup : collect.entrySet()) {
            // 获取店铺类型
            Long shopType = shopGroup.getKey();
            // 获取该类型下的所有店铺信息
            List<Shop> shops = shopGroup.getValue();
            // 根据店铺类型存入到redis中的key
            String key = RedisConstants.SHOP_GEO_KEY + shopType;
            List<RedisGeoCommands.GeoLocation<String>> localtions = new ArrayList<>(shops.size());
            for (Shop shop : shops) {
                // 设置店铺的地理信息
                RedisGeoCommands.GeoLocation<String> geoLocation =
                        new RedisGeoCommands.GeoLocation<>(
                                shop.getId().toString(), new Point(shop.getX(), shop.getY())
                        );
                // 放入到locaion列表中
                localtions.add(geoLocation);
            }
            // 存入到redis
            stringRedisTemplate.opsForGeo().add(key, localtions);
        }

    }

}
