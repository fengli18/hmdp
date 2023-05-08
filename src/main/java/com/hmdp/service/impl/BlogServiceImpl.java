package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

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
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(blogKey, userId.toString());
        if (BooleanUtil.isFalse(isMember)) {
            // 若没有点赞过 则可以进行点赞
            boolean isLikeSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isLikeSuccess) {
                // 判断点赞是否成功
                //若成功 则从redis中添加该用户 添加到该博文为键的集合中
                stringRedisTemplate.opsForSet().add(blogKey, userId.toString());
            }
        }else {
            // 若该用户已经点过赞了 则取消点赞
            boolean isLikeSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isLikeSuccess) {
                // 判断取消点赞是否成功
                //若成功 则从redis中在该博文为键的集合中删除该用户
                stringRedisTemplate.opsForSet().remove(blogKey, userId.toString());
            }
        }
        return Result.ok();
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
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(blogKey, userId.toString());
        blog.setIsLike(BooleanUtil.isTrue(isMember));
    }
}
