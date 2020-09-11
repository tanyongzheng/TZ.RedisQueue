# TZ.RedisQueue

[![nuget](https://img.shields.io/nuget/v/TZ.RedisQueue.svg?style=flat-square)](https://www.nuget.org/packages/TZ.RedisQueue) 
[![stats](https://img.shields.io/nuget/dt/TZ.RedisQueue.svg?style=flat-square)](https://www.nuget.org/stats/packages/TZ.RedisQueue?groupby=Version)
[![License](https://img.shields.io/badge/license-Apache2.0-blue.svg)](https://github.com/tanyongzheng/TZ.RedisQueue/blob/master/LICENSE)

## 介绍
基于Redis的List，ZSet实现的有序/去重队列


主要功能：
1. 设置过期时间
2. 队列内排序
3. 队列内去重


## 使用说明

1. Install-Package TZ.RedisQueue

2. 注入服务：
```    
    //配置Redis
    services.AddRedisQueue(Configuration);
    //services.AddRedisQueue(2, "127.0.0.1", 6379, null);
    //单例注入
    services.AddSingleton<RedisQueueService>();
```

3. 配置Redis
```
  "RedisQueueOptions": {
    "DefaultDatabase": 2,
    "Host": "127.0.0.1",
    "Port": 6379,
    "Password": null
  }
```

4. 使用见项目Demo
