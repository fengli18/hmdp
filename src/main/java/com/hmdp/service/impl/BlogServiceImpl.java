package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Resource
    private IFollowService followService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryBlogByid(Long id) {
        // 从数据库查询博文信息
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("当前博文不存在！");
        }
        // 查询blog有关用户信息
        queryUserInfo(blog);
        // 查询该文章是否被点赞
        queryIsLiked(blog);
        return Result.ok(blog);
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryUserInfo(blog);
            this.queryIsLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    @Transactional
    public Result likeBlog(Long id) {
        // 获取用户ID和博文点赞的redis键
        Long userId = UserHolder.getUser().getId();
        String blogKey = RedisConstants.BLOG_LIKED_KEY + id;
        // 从redis中查询该用户是否点赞过该博文
        Double score = stringRedisTemplate.opsForZSet().score(blogKey, userId.toString());
        if (score == null) {
            // 若没有点赞过 则可以进行点赞
            boolean isLikeSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isLikeSuccess) {
                // 判断点赞是否成功
                //若成功 则从redis中添加该用户 添加到该博文为键的集合中
                stringRedisTemplate.opsForZSet().add(blogKey, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 若该用户已经点过赞了 则取消点赞
            boolean isLikeSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isLikeSuccess) {
                // 判断取消点赞是否成功
                //若成功 则从redis中在该博文为键的集合中删除该用户
                stringRedisTemplate.opsForZSet().remove(blogKey, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String blogLikeKey = RedisConstants.BLOG_LIKED_KEY + id;
        // 查询点赞的前五位
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(blogLikeKey, 0, 4);
        // 判断是否有用户点赞
        if (top5 == null || top5.isEmpty()) {
            return Result.ok();
        }
        // 获取到点赞用户的id
        List<Long> usersId = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String join = StrUtil.join(",", usersId);
        // 转换为UserDto
        List<UserDTO> userDTOS = userService.query().in("id", usersId)
                .last("order by field(id," + join + ")").list()
                .stream()
                .map(item -> BeanUtil.copyProperties(item, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean saveSuccess = this.save(blog);
        if (!saveSuccess) {
            // 若未保存成功 则返回消息
            return Result.fail("博文保存失败！");
        }
        // 查询粉丝id
        List<Follow> fansIds = followService.lambdaQuery()
                .eq(Follow::getFollowUserId, user.getId())
                .list();
        fansIds.forEach(fansId -> {
            // 获取粉丝ID
            Long fans = fansId.getUserId();
            // 这是feedKey
            String feedKey = RedisConstants.FEED_KEY + fans;
            // 存入到redis中（粉丝收件箱） 以时间戳作为排序
            stringRedisTemplate.opsForZSet().add(feedKey, blog.getId().toString(), System.currentTimeMillis());
        });
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryUserFollowMsg(Long lastId, int offset) {
        Long loginUser = UserHolder.getUser().getId();
        String feedKey = RedisConstants.FEED_KEY + loginUser;
        // 从redis的收件箱查询消息
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(feedKey, 0, lastId, offset, 3);
        if (typedTuples == null || typedTuples.isEmpty()) {
            // 判断是否有消息 若没有 则返回空
            return Result.ok();
        }
        // 存储消息博文id
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long lastTime = 0;
        int ofs = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            String value = typedTuple.getValue();
            ids.add(Long.valueOf(value));
            long score = typedTuple.getScore().longValue();
            // 判断分数是否是最小的那个 如果是 判断有几个 则ofs+1
            if (score == lastTime) {
                ofs++;
            } else {
                lastTime = score;
                ofs = 1;
            }
        }
        // 根据博文的id查询博文的信息
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        // 查询博客是否被点赞 和该博文的博主信息
        for (Blog blog : blogs) {
            // 5.1.查询blog有关的用户
            queryUserInfo(blog);
            // 5.2.查询blog是否被点赞
            queryIsLiked(blog);
        }
        // 返回的结果信息
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(ofs);
        scrollResult.setMinTime(lastTime);
        return Result.ok(scrollResult);
    }


    /**
     * 获取博文的博主信息
     * @param blog
     */
    private void queryUserInfo(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    /**
     * 查询用户是否点赞过该博文 若点赞过 则设置为点赞状态
     * @param blog
     */
    private void queryIsLiked(Blog blog) {
        Long userId = blog.getUserId();
        String blogKey = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        // 从redis中查询该用户是否点赞过该博文
        Double isMember = stringRedisTemplate.opsForZSet().score(blogKey, userId.toString());
        blog.setIsLike(isMember != null);
    }
}
