package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisClientUtil;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
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
        Shop shop = redisClientUtil.queryWithRedisPass(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, (id2) -> getById(id2), RedisConstants.CACHE_SHOP_TTL, TimeUnit.SECONDS);

        // Shop shop = redisClientUtil
        //         .queryByLogicExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.SECONDS);
        if (shop == null) {
            return Result.fail("该商店不存在");
        }

        return Result.ok(shop);
    }


    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺ID不能为空！");
        }
        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
        // 删除redis中的缓存
        stringRedisTemplate.delete(shopKey);
        // 更新数据库
        updateById(shop);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否需要根据经纬度来进行查询
        if (x == null || y == null) {
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 设置分页数据
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 设置redis查询的key
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        // 从redis中获取该类型的所有商品信息
        GeoResults<RedisGeoCommands.GeoLocation<String>> search = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(3000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        // 若没有查到商品信息 则返回空
        if (search == null) {
            return Result.ok();
        }
        // 获取商铺的信息(name,point)
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> shopIds = search.getContent();
        if (shopIds.size() <= from) {
            // 没有下一页了，结束
            return Result.ok();
        }
        // 设置店铺的id和距离集合
        List<Long> ids = new ArrayList<>(shopIds.size());
        Map<String, Distance> distanceMap = new HashMap<>(shopIds.size());
        // 开始逻辑分页
        shopIds.stream().skip(from).forEach(item -> {
            // 获取店铺ID
            String idStr = item.getContent().getName();
            // 保存到ids列表
            ids.add(Long.valueOf(idStr));
            // 获取距离信息
            Distance distance = item.getDistance();
            // 放入到店铺和距离的map中
            distanceMap.put(idStr, distance);
        });
        // 根据id查询shop的信息
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        shops.forEach(shop -> {
            // 设置distance
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        });
        return Result.ok(shops);
    }
}
