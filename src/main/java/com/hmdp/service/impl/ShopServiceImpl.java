package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisClientUtil;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private RedisClientUtil redisClientUtil;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXCUTOR = Executors.newFixedThreadPool(10);

    /**
     * 查询商铺信息 by ID
     * 先去Redis中查询 若查询不到 再去数据库查询，然后写到Redis中
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {
        // Shop shop = queryByLock(id);  互斥锁
        // 逻辑过期
        // Shop shop = queryByLogicExpire(id);
        Shop shop = redisClientUtil.queryWithRedisPass(RedisConstants.CACHE_SHOP_KEY,id,Shop.class, (id2) -> getById(id2),RedisConstants.CACHE_SHOP_TTL, TimeUnit.SECONDS);

        // Shop shop = redisClientUtil
        //         .queryByLogicExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.SECONDS);
        if (shop == null){
            return Result.fail("该商店不存在");
        }

        return Result.ok(shop);
    }


    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺ID不能为空！");
        }
        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
        // 删除redis中的缓存
        stringRedisTemplate.delete(shopKey);
        // 更新数据库
        updateById(shop);
        return Result.ok();
    }
}
