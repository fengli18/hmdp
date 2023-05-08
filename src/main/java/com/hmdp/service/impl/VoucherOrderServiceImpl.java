package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckillScript.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static ExecutorService SECKILL_EXCUTORS = Executors.newSingleThreadExecutor();

    private IVoucherOrderService proxy;

    /**
     * 在类加载完成之后 让线程提交任务
     */
    @PostConstruct
    private void init() {
        SECKILL_EXCUTORS.submit(new ExcutorTask());
    }

    /**
     * 线程提交的任务
     */
    private class ExcutorTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取队列中的订单任务
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 处理订单
                    handlerOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("订单异常信息:", e);
                }
            }
        }
    }

    /**
     * 消息队列订单处理方法
     * @param voucherOrder
     */
    private void handlerOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock(RedisConstants.LOCK_KEY + "order:" + userId);
        try {
            // 获取redis锁
            boolean lock = redisLock.tryLock();
            if (!lock) {
                log.error("不允许重复下单！");
                return;
            }
            // 根据代理对象创建订单信息 (添加到数据库中)
            proxy.createOrder(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户ID
        Long userId = UserHolder.getUser().getId();
        // 执行脚本
        Long executeResult = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        // 判断脚本执行结果是否为0
        int i = executeResult.intValue();
        if (i != 0) {
            // 不为0 没有购买资格
            return Result.fail(i == 1 ? "库存不足" : "不能重复下单");
        }
        // 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order:");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);
        // 添加到订单队列里
        orderTasks.add(voucherOrder);
        // 获取IVoucherOrderService代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

/*     @Override
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
        // RedisLock redisLock = new RedisLockImpl(stringRedisTemplate, "order:" + userId);
        RLock redisLock = redissonClient.getLock(RedisConstants.LOCK_KEY + "order:" + userId);
        try {
            // 获取redis锁
            boolean lock = redisLock.tryLock();
            if (!lock) {
                return Result.fail("不允许重复下单！");
            }
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createOrder(voucherId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    } */

    /**
     * 在数据库中创建订单
     * @param voucherOrder
     */
    @Transactional
    public void createOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 查询当前用户是否下订单
        int count = lambdaQuery()
                .eq(VoucherOrder::getUserId, userId)
                .eq(VoucherOrder::getVoucherId, voucherOrder.getVoucherId())
                .count();
        if (count > 0) {
            log.error("当前用户已经购买过一次！");
            return;
        }
        // 进行乐观锁设置
        boolean update = seckillVoucherService.update()
                .setSql("stock = stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!update) {
            log.error("库存不足！");
            return;
        }
        // 添加秒杀代金券订单 添加到数据库中
        save(voucherOrder);
    }
}
