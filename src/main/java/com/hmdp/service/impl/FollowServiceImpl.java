package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long id, boolean isFollow) {
        // 获取登录用户ID
        Long userId = UserHolder.getUser().getId();
        String followKey = RedisConstants.FOLLOW_KEY + userId;
        if (isFollow) {
            // 若用户是关注操作则保存到数据库
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(id);
            boolean saveSuccess = this.save(follow);
            // 保存到redis的set中 方便查询共同关注
            if (saveSuccess) {
                // 若保存到数据库成功 则保存到redis
                stringRedisTemplate.opsForSet().add(followKey, id.toString());
            }
        } else {
            // 取关
            LambdaQueryWrapper<Follow> lq = new LambdaQueryWrapper<>();
            lq.eq(Follow::getUserId, userId).eq(Follow::getFollowUserId, id);
            boolean removeSuccess = this.remove(lq);
            if (removeSuccess) {
                // 判断是否删除成功 成功的话才从redis中移除
                stringRedisTemplate.opsForSet().remove(followKey, id.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long id) {
        // 获取登录用户ID
        Long userId = UserHolder.getUser().getId();
        LambdaQueryWrapper<Follow> lq = new LambdaQueryWrapper<>();
        lq.eq(Follow::getUserId, userId).eq(Follow::getFollowUserId, id);
        int count = this.count(lq);
        return Result.ok(count > 0);
    }

    @Override
    public Result commonFollow(Long id) {
        // 获取当前登录用户的id
        Long userId = UserHolder.getUser().getId();
        // 获取关注的key
        String followKey1 = RedisConstants.FOLLOW_KEY + userId;
        String followKey2 = RedisConstants.FOLLOW_KEY + id;
        // 从关注里面取交集就能得到共同关注
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(followKey1, followKey2);
        if (intersect == null || intersect.isEmpty()) {
            // 如果没有共同关注 则返回空
            return Result.ok();
        }
        // 获取共同关注的id
        List<Long> commonFollowID = intersect.stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        // 从数据库查询数据转换为DTO
        List<User> users = userService.listByIds(commonFollowID);
        List<UserDTO> collect = users.stream()
                .map(item -> BeanUtil.copyProperties(item, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(collect);
    }
}
