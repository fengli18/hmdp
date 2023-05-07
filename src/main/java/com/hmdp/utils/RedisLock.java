package com.hmdp.utils;

public interface RedisLock {
    boolean getLock(long timeout);
    void delLock();
}
