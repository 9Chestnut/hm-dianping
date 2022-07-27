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
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

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
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1。检验手机号
        if (RegexUtils.isPhoneInvalid(phone)){
            // 2。如果不符合，返回错误信息

            return Result.fail("手机号码格式错误！");
        }
        // 3。符合生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 4。保存验证码到 redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // 5。发送验证码
        log.debug("发送短信验证码成功，验证码：{}",code);
        return null;
    }

    @Override
    public Object login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        // 1。检验手机号
        if (RegexUtils.isPhoneInvalid(phone)){
            // 2。如果不符合，返回错误信息

            return Result.fail("手机号码格式错误！");
        }
        // 2.校验验证码,从redis中获取
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode ==null || !cacheCode.equals(code)){
            // 3.验证码不一致，报错
            return Result.fail("验证码错误！");
        }
        // 4.验证码一直，根据手机号查询用户信息
        User user = query().eq("phone", phone).one();
        // 5.判断用户是否存在
        if (null == user){
            // 6.用户不存在，创建新用户并保存
           user = createUserWithUSer(phone);

        }
        // 7.保存用户信息到 redis 中
        // 7.1 随机生成 Token ，作为登陆令牌
        String token = UUID.randomUUID().toString(true);
        // 7。2 将 User 对象转换成 hashMap 类型
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));
        // 7。3 存储到 redis
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
        // 7.4 设置 token 有效期
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);
        // 8。 返回 Token 给前端
        return Result.ok(token);
    }

    private User createUserWithUSer(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
