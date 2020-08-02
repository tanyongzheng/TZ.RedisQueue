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
        private readonly static string minutesFormatKeySuffix = "yyyy-MM-dd_HH:mm";

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


        #region 按小时做Key的List队列，整个消息过期是按照Key算，同步方法

        #region 基础方法
        /// <summary>
        /// 发送消息到队列，使用List实现，有重复
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="keyExpiryTimeType">Key过期时间类型，默认为小时</param>
        /// <param name="expiryTimes">key过期时间类型的倍数，如过期时间是按小时，则表示多少小时后过期</param>
        /// <returns>返回是否发送成功</returns>
        private bool SendQueueWithKeyExpiry(string queueKeyPrefix,
            string msg,
            KeyExpiryTimeType keyExpiryTimeType = KeyExpiryTimeType.Hours,
            int expiryTimes = 2)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryTimes <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryTimes)} must be greater than 1 !");
            }
            TimeSpan expiry = GetKeyExpiryTime(keyExpiryTimeType, expiryTimes);

            lock (ListWriteObj)
            {
                RedisKey queueKey = GetQueueKey(keyExpiryTimeType, RedisDataType.List, queueKeyPrefix);
                var keyExists = redis.GetDatabase().KeyExists(queueKey);
                RedisValue queueItems = msg;
                //var aaa = redis.GetDatabase().List(queueKey, queueItems, When.NotExists);
                var when = When.Always;//List 不能用NotExists
                var pushResult = redis.GetDatabase().ListLeftPush(queueKey, queueItems, when);
                if (!keyExists&&pushResult>0)
                {
                    redis.GetDatabase().KeyExpire(queueKey, expiry);
                }
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
        private List<string> GetMessageByKeyExpiry(string queueKeyPrefix,
            int count = 1,
            KeyExpiryTimeType keyExpiryTimeType = KeyExpiryTimeType.Hours
            )
        {
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            var msgList = new List<string>();
            var queueKeyPattern = GetQueueKeyPattern(keyExpiryTimeType, RedisDataType.List, queueKeyPrefix);
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
            return SendQueueWithKeyExpiry(queueKeyPrefix, msg, KeyExpiryTimeType.Hours, expiryHours);
        }

        /// <summary>
        /// 发送消息到队列，使用List实现，有重复
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="expiryMinutes">过期分钟</param>
        /// <returns>返回是否发送成功</returns>
        public bool SendMinutesQueue(string queueKeyPrefix,string msg, int expiryMinutes = 2)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryMinutes <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryMinutes)} must be greater than 1 !");
            }
            return SendQueueWithKeyExpiry(queueKeyPrefix, msg, KeyExpiryTimeType.Minutes, expiryMinutes);
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
            return GetMessageByKeyExpiry(queueKeyPrefix, count,KeyExpiryTimeType.Hours);
        }


        /// <summary>
        /// 接收队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <returns>返回消息列表</returns>
        public List<string> GetMinutesMessage(string queueKeyPrefix, int count=1)
        {
            return GetMessageByKeyExpiry(queueKeyPrefix, count,KeyExpiryTimeType.Minutes);
        }
        #endregion


        #region 按小时做Key的ZSET排序队列，无重复，整个消息过期是按照Key算，同步方法

        #region 基础方法
        /// <summary>
        /// 发送消息到排序队列，使用ZSET，同一Key内无重复
        /// 排序按照时间戳升序
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="keyExpiryTimeType">Key过期时间类型，默认为小时</param>
        /// <param name="expiryTimes">key过期时间类型的倍数，如过期时间是按小时，则表示多少小时后过期</param>
        /// <returns>返回是否发送成功</returns>
        private bool SendSortQueueWithKeyExpiry(string queueKeyPrefix, 
            string msg,
            KeyExpiryTimeType keyExpiryTimeType = KeyExpiryTimeType.Hours,
            int expiryTimes = 2)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryTimes <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryTimes)} must be greater than 1 !");
            }
            TimeSpan expiry = GetKeyExpiryTime(keyExpiryTimeType, expiryTimes);
            lock (ZSetWriteObj)
            {
                RedisKey queueKey = GetQueueKey(keyExpiryTimeType, RedisDataType.ZSet, queueKeyPrefix);
                var keyExists = redis.GetDatabase().KeyExists(queueKey);
                RedisValue queueItems = msg;
                var score = DateTime.Now.Ticks;
                var when = When.NotExists;
                var addResult = redis.GetDatabase().SortedSetAdd(queueKey, queueItems, score, when);
                if (!keyExists&&addResult)
                {
                    redis.GetDatabase().KeyExpire(queueKey, expiry);
                }
                return addResult;
            }
        }


        /// <summary>
        /// 接收排序队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <param name="keyExpiryTimeType">Key过期时间类型，默认为小时</param>
        /// <returns>返回消息列表</returns>
        private List<string> GetSortMessageByKeyExpiry(
            string queueKeyPrefix, 
            int count = 1,
            KeyExpiryTimeType keyExpiryTimeType = KeyExpiryTimeType.Hours
            )
        {
            bool getBySingle = false;
            var msgList = new List<string>();
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            var queueKeyPattern = GetQueueKeyPattern(keyExpiryTimeType, RedisDataType.ZSet, queueKeyPrefix);
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
            return SendSortQueueWithKeyExpiry(queueKeyPrefix, msg, KeyExpiryTimeType.Hours, expiryHours);
        }

        /// <summary>
        /// 发送消息到排序队列，使用ZSET，同一Key内无重复
        /// 排序按照时间戳升序
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="expiryMinutes">过期分钟</param>
        /// <returns>返回是否发送成功</returns>
        public bool SendMinutesSortQueue(string queueKeyPrefix, string msg, int expiryMinutes = 2)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryMinutes <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryMinutes)} must be greater than 1 !");
            }
            return SendSortQueueWithKeyExpiry(queueKeyPrefix, msg, KeyExpiryTimeType.Minutes, expiryMinutes);
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
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            return GetSortMessageByKeyExpiry(queueKeyPrefix, count, KeyExpiryTimeType.Hours);
        }

        /// <summary>
        /// 接收排序队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <returns>返回消息列表</returns>
        public List<string> GetMinutesSortMessage(string queueKeyPrefix, int count = 1)
        {
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            return GetSortMessageByKeyExpiry(queueKeyPrefix, count, KeyExpiryTimeType.Minutes);
        }

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
        

        private TimeSpan GetKeyExpiryTime(KeyExpiryTimeType keyExpiryTimeType, int expiryTimes)
        {
            TimeSpan expiry = TimeSpan.FromHours(expiryTimes);
            if (keyExpiryTimeType == KeyExpiryTimeType.Hours)
            {
                expiry = TimeSpan.FromHours(expiryTimes);
            }
            else if (keyExpiryTimeType == KeyExpiryTimeType.Minutes)
            {
                expiry = TimeSpan.FromMinutes(expiryTimes);
            }
            return expiry;
        }

        private RedisKey GetQueueKey(KeyExpiryTimeType keyExpiryTimeType,RedisDataType redisDataType, string queueKeyPrefix)
        {
            var currentTime = DateTime.Now.ToString(hoursFormatKeySuffix);
            var timesNodeStr = "Hours";
            if (keyExpiryTimeType == KeyExpiryTimeType.Hours)
            {
                currentTime = DateTime.Now.ToString(hoursFormatKeySuffix);
                timesNodeStr = "Hours";
            }
            else if (keyExpiryTimeType == KeyExpiryTimeType.Minutes)
            {
                currentTime = DateTime.Now.ToString(minutesFormatKeySuffix);
                timesNodeStr = "Minutes";
            }

            RedisKey queueKey = queueKeyPrefix + (redisDataType==RedisDataType.List? listQueueKeyPrefix:zsetQueueKeyPrefix) + timesNodeStr+":" + currentTime;
            return queueKey;
        }

        private string GetQueueKeyPattern(KeyExpiryTimeType keyExpiryTimeType,RedisDataType redisDataType,string queueKeyPrefix)
        {
            var queueKeyPattern = queueKeyPrefix + (redisDataType == RedisDataType.List ? listQueueKeyPrefix : zsetQueueKeyPrefix);

            var timesNodeStr = "Hours";
            var timesFormatKeySuffix = "";
            if (keyExpiryTimeType == KeyExpiryTimeType.Hours)
            {
                timesNodeStr = "Hours";
                timesFormatKeySuffix=hoursFormatKeySuffix.Replace("yyyy", "*").
                Replace("MM", "*").
                Replace("dd", "*").
                Replace("HH", "*")
                ;
            }
            else if (keyExpiryTimeType == KeyExpiryTimeType.Minutes)
            {
                timesNodeStr = "Minutes";
                timesFormatKeySuffix = minutesFormatKeySuffix.Replace("yyyy", "*").
                Replace("MM", "*").
                Replace("dd", "*").
                Replace("HH", "*").Replace("mm", "*");
            }
            queueKeyPattern += timesNodeStr + ":"+timesFormatKeySuffix;
            
            return queueKeyPattern;
        }
        #endregion

    }
}
