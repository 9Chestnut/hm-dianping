package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;

    // 初始化 Lua 脚本

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 阻塞队列
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    // 线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    /**
     *   当前类初始化完毕之后就会执行
     */
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VocherOrderhandler());
    }


    // 线程任务
    private class VocherOrderhandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //  1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2。创建订单
                    handlerVocherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                }
            }
        }
        
    }

    private void handlerVocherOrder(VoucherOrder voucherOrder) {
        // 获取用户id
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock){
            // 获取锁失败，返回错误
            log.error("不允许重复下单！");
            return ;
        }

        // 获取代理对象（事务才能生效）
        try {

              proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }

    }

private IVoucherOrderService proxy;
    /**
     * lua 脚本版，异步下单
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        // 2.判断结果是否为0
        int r = result.intValue();
        if (r != 0){
            // 2.1 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2 为0，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        // 把形成的订单放入阻塞队列
        orderTasks.add(voucherOrder);

        // 获取代理对象
         proxy = (IVoucherOrderService) AopContext.currentProxy();


        // 3.返回订单id
        return Result.ok(orderId);


    }

    /**
     * 单机版
     *
     * @param voucherOrder
     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//
//        // 1.查询优惠券
//        SeckillVoucher vocher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if (vocher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始！");
//        }
//        // 3.判秒杀是否结束
//        if (vocher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束！");
//        }
//        // 4.判断库存是否充足
//        if (vocher.getStock() < 1) {
//           ·· return Result.fail("库存不足！");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        /**
//         *  只需要给同一个用户加锁
//         *  toString() 方法，每次都回去 new 新的对象，对象变了锁也就变了
//         *  所以使用 intern() 方法
//         *  intern() 每次都去常量池找有没有这个对象，有就使用，没有才会创建
//         *  这样就回保证当用户 ID 的值一样的情况下，锁就一样
//         */
////        synchronized (userId.toString().intern()) {
//
////        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        if (!isLock){
//            // 获取锁失败，返回错误
//            return Result.fail("不允许重复下单！");
//        }
//
//        // 获取代理对象（事务才能生效）
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
////        }
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 5.一人一单
        Long userId = voucherOrder.getUserId();

        // 5.1 查询订单
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder).count();
        // 5.2 判断订单是否存在
        if (count > 0) {
            log.error("用户已经购买过一次了！不允许再抢购！");
            return;
        }

        // 6.扣减库存
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1").eq("voucher_id", voucherOrder).gt("stock", 0).update();
        if (!success) {
            log.error("扣减库存失败");
            return;
        }
        // 7.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(UserHolder.getUser().getId());
//        voucherOrder.setVoucherId(voucherOrder);
        save(voucherOrder);
//        return Result.ok(voucherOrder);


    }
}
