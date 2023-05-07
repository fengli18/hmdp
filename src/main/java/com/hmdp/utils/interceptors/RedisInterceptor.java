package com.hmdp.utils.interceptors;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RedisInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RedisInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        // 获取token的值
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)){
            return true;
        }
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        // 从redis中获取用户信息
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(tokenKey);
        if (userMap.isEmpty()){
            return true;
        }
        // 将map转换为userdto
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        UserHolder.saveUser(userDTO);
        // 刷新token过期
        stringRedisTemplate.expire(tokenKey,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 释放 防止内存泄露
        UserHolder.removeUser();
    }
}
