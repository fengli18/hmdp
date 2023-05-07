package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.RedisLock;
import com.hmdp.utils.UserHolder;
import com.hmdp.utils.lockImpl.RedisLockImpl;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 * 服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result seckillVoucher(Long voucherId) {
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        // 判断秒杀时间是否开始？
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始！请关注开始时间");
        }
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已结束！请早点来参与");
        }
        // 获取库存
        int stock = seckillVoucher.getStock();
        if (stock < 1) {
            return Result.fail("库存不足！");
        }
        Long userId = UserHolder.getUser().getId();
        // 创建锁对象
        RedisLock redisLock = new RedisLockImpl(stringRedisTemplate, "order:" + userId);
        try {
            // 获取redis锁
            boolean lock = redisLock.getLock(1200);
            if (!lock){
                return Result.fail("不允许重复下单！");
            }
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createOrder(voucherId);
        } finally {
            // 释放锁
            redisLock.delLock();
        }

    }

    @Transactional
    public Result createOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 查询当前用户是否下订单
        int count = lambdaQuery()
                .eq(VoucherOrder::getUserId, userId)
                .eq(VoucherOrder::getVoucherId, voucherId)
                .count();
        if (count > 0) {
            return Result.fail("当前用户已经购买过一次！");
        }
        // 进行乐观锁设置
        boolean update = seckillVoucherService.update()
                .setSql("stock = stock -1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!update) {
            return Result.fail("库存不足！");
        }
        // 添加代金券订单 添加到数据库中
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order:");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        save(voucherOrder);
        // 返回前端信息
        return Result.ok(orderId);
    }
}
