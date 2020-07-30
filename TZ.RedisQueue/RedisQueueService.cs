using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TZ.RedisQueue.Configurations;

namespace TZ.RedisQueue
{
    public partial class RedisQueueService
    {
        private static ConnectionMultiplexer redis;
        private static readonly object lockObj = new object();
        private static readonly object ListWriteObj = new object();
        private static readonly object ZSetWriteObj = new object();
        private static readonly object ZSetReadObj = new object();

        private readonly RedisQueueOptions _RedisQueueOptions;
        private readonly List<IServer> serverList;

        private readonly static string listQueueKeyPrefix = "_List_";
        private readonly static string zsetQueueKeyPrefix = "_ZSet_";

        private readonly static string hoursFormatKeySuffix = "yyyy-MM-dd_HH";

        //private static int redisMainVersion;
        private static Version RedisServerVersion;

        /// <summary>
        /// 程序中最好使用单例模式
        /// 如要新建对象实例，请在程序最开始的地方先实例化一个对象
        /// </summary>
        /// <param name="options"></param>
        public RedisQueueService(IOptions<RedisQueueOptions> options)
        {
            if (options == null || options.Value == null)
            {
                throw new Exception("please set RedisQueueOptions!");
            }
            else if (options.Value != null)
            {
                _RedisQueueOptions = options.Value;
            }
            if (_RedisQueueOptions == null)
            {
                throw new Exception("please set RedisQueueOptions!");
            }
            if (_RedisQueueOptions.DefaultDatabase < 0)
            {
                throw new Exception("please set RedisQueueOptions->DefaultDatabase !");
            }
            if (string.IsNullOrEmpty(_RedisQueueOptions.Host))
            {
                throw new Exception("please set RedisQueueOptions-> Host!");
            }
            if (_RedisQueueOptions.Port < 0)
            {
                throw new Exception("please set RedisQueueOptions-> Port!");
            }
            /*
            if (string.IsNullOrEmpty(_RedisQueueOptions.RedisVersion))
            {
                throw new Exception("please set RedisQueueOptions-> RedisVersion!");
            }
            var mainVersionStr = _RedisQueueOptions.RedisVersion;
            if (_RedisQueueOptions.RedisVersion.Contains("."))
            {
                mainVersionStr = _RedisQueueOptions.RedisVersion.Split('.')[0];
            }
            if (!int.TryParse(mainVersionStr,out var mainVersion))
            {
                throw new Exception($"RedisQueueOptions-> RedisVersion, Version [{_RedisQueueOptions.RedisVersion}] format error");
            }
            redisMainVersion = mainVersion;
            */
            if (redis == null)
            {
                lock (lockObj)
                {
                    if (redis == null)
                    {
                        //初始化redis
                        InitRedis();
                        serverList = GetServers();
                        //redisMainVersion = serverList[0].Version.Major;
                        RedisServerVersion= serverList[0].Version;
                    }
                }
            }
            
        }


        #region 按小时做Key的List队列，整个消息过期是按照Key小时算，同步方法

        /// <summary>
        /// 发送消息到队列，使用List实现，有重复
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="expiryHours">过期小时</param>
        /// <returns>返回是否发送成功</returns>
        public bool SendHoursQueue(string queueKeyPrefix,string msg, int expiryHours = 2)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryHours <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryHours)} must be greater than 1 !");
            }
            TimeSpan expiry = TimeSpan.FromHours(expiryHours);
            lock (ListWriteObj)
            {
                var currentHour = DateTime.Now.ToString(hoursFormatKeySuffix);
                RedisKey queueKey = queueKeyPrefix + listQueueKeyPrefix + currentHour;
                var keyExists = redis.GetDatabase().KeyExists(queueKey);
                if (!keyExists)
                {
                    redis.GetDatabase().KeyExpire(queueKey, expiry);
                }
                RedisValue queueItems = msg;
                //var aaa = redis.GetDatabase().List(queueKey, queueItems, When.NotExists);
                var when = When.Always;//List 不能用NotExists
                var pushResult = redis.GetDatabase().ListLeftPush(queueKey, queueItems, when);
                return pushResult > 0;
            }
        }


        /// <summary>
        /// 接收队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <returns>返回消息列表</returns>
        public List<string> GetHoursMessage(string queueKeyPrefix, int count=1)
        {
            var msgList = new List<string>();
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }

            var queueKeyPattern = queueKeyPrefix + listQueueKeyPrefix +
                hoursFormatKeySuffix.Replace("yyyy", "*").
                Replace("MM", "*").
                Replace("dd", "*").
                Replace("HH", "*")
                ;
            var keyList = GetkeysByPrefix(queueKeyPattern);

            foreach (var item in keyList)
            {
                RedisKey queueKey = item;
                var keyExists = redis.GetDatabase().KeyExists(queueKey);
                if (!keyExists)
                {
                    continue;
                }
                while (msgList.Count < count && redis.GetDatabase().ListLength(queueKey) > 0)
                {
                    var msgV = redis.GetDatabase().ListRightPop(queueKey);
                    msgList.Add(msgV);
                    if (msgList.Count == count)
                    {
                        return msgList;
                    }
                }
            }
            return msgList;
        }


        #endregion


        #region 按小时做Key的ZSET排序队列，无重复，整个消息过期是按照Key小时算，同步方法

        #region 同步方法
        /// <summary>
        /// 发送消息到排序队列，使用ZSET，同一Key内无重复
        /// 排序按照时间戳升序
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="expiryHours">过期小时</param>
        /// <returns>返回是否发送成功</returns>
        public bool SendHoursSortQueue(string queueKeyPrefix, string msg, int expiryHours = 2)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryHours <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryHours)} must be greater than 1 !");
            }
            TimeSpan expiry = TimeSpan.FromHours(expiryHours);
            lock (ZSetWriteObj)
            {
                var currentHour = DateTime.Now.ToString(hoursFormatKeySuffix);
                RedisKey queueKey = queueKeyPrefix + zsetQueueKeyPrefix + currentHour;
                var keyExists = redis.GetDatabase().KeyExists(queueKey);
                if (!keyExists)
                {
                    redis.GetDatabase().KeyExpire(queueKey, expiry);
                }
                RedisValue queueItems = msg;
                var score = DateTime.Now.Ticks;
                var when = When.NotExists;
                var addResult = redis.GetDatabase().SortedSetAdd(queueKey, queueItems, score, when);
                return addResult;
            }
        }


        /// <summary>
        /// 接收排序队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <returns>返回消息列表</returns>
        public List<string> GetHoursSortMessage(string queueKeyPrefix, int count = 1)
        {
            bool getBySingle = false;
            var msgList = new List<string>();
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            var queueKeyPattern = queueKeyPrefix + zsetQueueKeyPrefix +
                hoursFormatKeySuffix.Replace("yyyy", "*").
                Replace("MM", "*").
                Replace("dd", "*").
                Replace("HH", "*")
                ;
            var keyList = GetkeysByPrefix(queueKeyPattern);
            foreach (var item in keyList)
            {
                RedisKey queueKey = item;
                var keyExists = redis.GetDatabase().KeyExists(queueKey);
                if (!keyExists)
                {
                    continue;
                }

                while (msgList.Count < count && redis.GetDatabase().SortedSetLength(queueKey) > 0)
                {
                    //ZPOPMIN命令需要Redis 5及以上版本SortedSetPop方法
                    //redis.GetDatabase().SortedSetScan(queueKey,, Order.Ascending);                    
                    if (getBySingle)
                    {

                        if (RedisServerVersion < new Version("5.0.0"))
                        {
                            var startIndex = 0;
                            var endIndex = 0;
                            var vList = redis.GetDatabase().SortedSetRangeByRank(queueKey, startIndex, endIndex, Order.Ascending);
                            if (vList == null || vList.Count() == 0)
                            {
                                break;
                            }
                            var msgV = vList.FirstOrDefault();
                            msgList.Add(msgV);
                            redis.GetDatabase().SortedSetRemove(queueKey, msgV);
                        }
                        else
                        {
                            var sortedSetEntry = redis.GetDatabase().SortedSetPop(queueKey, Order.Ascending);
                            if (!sortedSetEntry.HasValue)
                            {
                                break;
                            }
                            var msgV = sortedSetEntry.Value.Element;
                            msgList.Add(msgV);
                        }
                        if (msgList.Count == count)
                        {
                            return msgList;
                        }
                    }
                    else
                    {
                        var getCount = count - msgList.Count;
                        if (getCount == 0)
                        {
                            return msgList;
                        }

                        if (RedisServerVersion < new Version("5.0.0"))
                        {
                            lock (ZSetReadObj)
                            {
                                var startIndex = 0;
                                var endIndex = getCount - 1;
                                var vList = redis.GetDatabase().SortedSetRangeByRank(queueKey, startIndex, endIndex, Order.Ascending);
                                if (vList == null || vList.Count() == 0)
                                {
                                    break;
                                }
                                 redis.GetDatabase().SortedSetRemove(queueKey, vList);
                                foreach (var msgV in vList)
                                {
                                    msgList.Add(msgV);
                                    //redis.GetDatabase().SortedSetRemove(queueKey, msgV);
                                }
                            }
                        }
                        else
                        {
                            var sortedSetEntryList = redis.GetDatabase().SortedSetPop(queueKey, getCount, Order.Ascending);
                            if (sortedSetEntryList == null || sortedSetEntryList.Length == 0)
                            {
                                break;
                            }
                            foreach (var sortedSetEntry in sortedSetEntryList)
                            {
                                var msgV = sortedSetEntry.Element;
                                msgList.Add(msgV);
                            }
                        }
                        if (msgList.Count == count)
                        {
                            return msgList;
                        }
                    }
                }
            }
            return msgList;
        }
        #endregion

        #endregion

        #region private method
        /// <summary>
        /// 初始化Redis连接
        /// </summary>
        private void InitRedis()
        {
            ConfigurationOptions configurationOptions = new ConfigurationOptions();
            configurationOptions.AbortOnConnectFail = false;//超时不重试
            configurationOptions.EndPoints.Add(_RedisQueueOptions.Host, _RedisQueueOptions.Port);
            if (!string.IsNullOrEmpty(_RedisQueueOptions.Password))
                configurationOptions.Password = _RedisQueueOptions.Password;
            configurationOptions.DefaultDatabase = _RedisQueueOptions.DefaultDatabase;
            //ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("server1:6379,server2:6379,abortConnect= false");
            redis = ConnectionMultiplexer.Connect(configurationOptions);
        }

        private List<IServer> GetServers()
        {
            var servers = new List<IServer>();
            var endPoints = redis.GetEndPoints();
            foreach (var endPoint in endPoints)
            {
                var server = redis.GetServer(endPoint);
                servers.Add(server);
            }
            return servers;
        }


        private List<string> GetkeysByPrefix(string queueKeyPrefix)
        {
            List<string> keyList = new List<string>();
            foreach (var server in serverList)
            {
                var keys = server.Keys(_RedisQueueOptions.DefaultDatabase, $"{queueKeyPrefix}*");
                foreach (var item in keys)
                {
                    keyList.Add(item);
                }
            }
            if (keyList.Count > 1)
            {
                //Key按日期升序排列
                keyList.Sort();
            }
            return keyList;
        } 
        
        #endregion

    }
}
