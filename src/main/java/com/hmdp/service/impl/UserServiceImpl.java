package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_CODE_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_CODE_TTL;

/**
 * <p>
 * 服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 实现发送验证码功能
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式有误，请重新输入！");
        }
        String code = RandomUtil.randomNumbers(6);
        // 将验证码存放到redis中 存放时间为2分钟
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // 输出验证码
        log.error("验证码是:{}", code);
        return Result.ok();
    }

    /**
     * 登录功能的实现
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 获取手机号和验证码
        String phone = loginForm.getPhone();
        String code = loginForm.getCode();
        // 验证手机号是否正确
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式有误！请重新输入");
        }
        // 从redis中获取验证码
        String code1 = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if (code1 == null || !code1.equals(code)) {
            return Result.fail("验证码有误！");
        }
        // 从数据库中获取user信息
        User user = lambdaQuery().eq(User::getPhone, phone).one();
        if (user == null) {
            user = createUser(phone);
        }
        // 生成一个token 并且生成tokenKey名和map
        String token = UUID.randomUUID().toString(true);
        // 用户的tokenKey
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions
                        .create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((String s, Object o) -> o.toString()));
        // 存入到redis中
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        stringRedisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        // 将token返回
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 获取当前登录用户id
        Long userId = UserHolder.getUser().getId();
        // 获取当前时间
        LocalDateTime now = LocalDateTime.now();
        String suffix = now.format(DateTimeFormatter.ofPattern(":yyyy/MM"));
        // 设置redis key
        String key = RedisConstants.USER_SIGN_KEY + userId + suffix;
        // 获取当前是这个月第几天
        int offset = now.getDayOfMonth();
        stringRedisTemplate.opsForValue().setBit(key, offset - 1, true);
        return Result.ok();
    }

    /**
     * 判断当前用户在本月到目前为止的连续签到时间
     * @return
     */
    @Override
    public Result signCount() {
        // 获取当前登录用户id
        Long userId = UserHolder.getUser().getId();
        // 获取当前时间
        LocalDateTime now = LocalDateTime.now();
        String suffix = now.format(DateTimeFormatter.ofPattern(":yyyy/MM"));
        // 设置redis key
        String key = RedisConstants.USER_SIGN_KEY + userId + suffix;
        // 获取当前是这个月第几天
        int offset = now.getDayOfMonth();
        // 获取本月到今天为止的签到信息(10进制无符号数)
        List<Long> longs = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(offset))
                        .valueAt(0)
        );
        // 若没有签到记录 则返回空数据
        if (longs == null || longs.isEmpty()) {
            return Result.ok();
        }
        Long sign = longs.get(0);
        // 获取签到记录形成的十进制数 判断是否存在
        if (sign == null || sign == 0) {
            return Result.ok(0);
        }
        int count = 0;
        while (true) {
            // 最后移位与1进行与运算 看是否为1
            if ((sign & 1) == 0) {
                // 不为1
                break;
            } else {
                // 为1 计数器加1
                count++;
            }
            // 向右移1位
            sign >>>= 1;
        }
        return Result.ok(count);
    }

    /**
     * 创建用户 当用户在数据库查不到时
     * @param phone
     * @return
     */
    private User createUser(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
