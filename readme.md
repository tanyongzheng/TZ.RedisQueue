# TZ.RedisQueue

Redis队列

#### 介绍
基于Redis的List，ZSet实现的有序/去重队列


#### 使用说明

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
