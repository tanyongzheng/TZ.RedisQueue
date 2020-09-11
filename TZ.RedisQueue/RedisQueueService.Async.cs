using Nito.AsyncEx;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TZ.RedisQueue
{
    public partial class RedisQueueService
    {

        private static readonly SemaphoreSlim ListWriteSyncSemaphore= new SemaphoreSlim(1, 1);
        //private static readonly SemaphoreSlim ListReadSyncSemaphore = new SemaphoreSlim(1, 1);
        private static readonly SemaphoreSlim ZSetWriteSyncSemaphore = new SemaphoreSlim(1, 1);
        private static readonly SemaphoreSlim ZSetReadSyncSemaphore = new SemaphoreSlim(1, 1);

        #region 按小时做Key的List队列，整个消息过期是按照Key算，异步方法

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
        private async Task<bool> SendQueueWithKeyExpiryAsync(string queueKeyPrefix,
            string msg,
            KeyExpiryTimeType keyExpiryTimeType = KeyExpiryTimeType.Hours,
            int expiryTimes = 2,
            CancellationToken token = default)
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
            //lock (lockObj)
            using (await ListWriteSyncSemaphore.LockAsync(token))
            {
                RedisKey queueKey = GetQueueKey(keyExpiryTimeType, RedisDataType.List, queueKeyPrefix);
                var keyExists = await redis.GetDatabase().KeyExistsAsync(queueKey);
                RedisValue queueItems = msg;
                //var aaa = redis.GetDatabase().List(queueKey, queueItems, When.NotExists);
                var when = When.Always;//List 不能用NotExists
                var pushResult = await redis.GetDatabase().ListLeftPushAsync(queueKey, queueItems, when);
                if (!keyExists&&pushResult>0)
                {
                    await redis.GetDatabase().KeyExpireAsync(queueKey, expiry);
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
        private async Task<List<string>> GetMessageByKeyExpiryAsync(string queueKeyPrefix,
            int count = 1,
            KeyExpiryTimeType keyExpiryTimeType = KeyExpiryTimeType.Hours)
        {
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            var msgList = new List<string>();
            var queueKeyPattern = GetQueueKeyPattern(keyExpiryTimeType, RedisDataType.List, queueKeyPrefix);

#if NETCOREAPP3_0 || NETCOREAPP3_1
            var keyList =await GetkeysByPrefixAsync(queueKeyPattern);
#else
            var keyList = GetkeysByPrefix(queueKeyPattern);
#endif
            foreach (var item in keyList)
            {
                RedisKey queueKey = item;
                var keyExists = await redis.GetDatabase().KeyExistsAsync(queueKey);
                if (!keyExists)
                {
                    continue;
                }
                while (msgList.Count < count && (await redis.GetDatabase().ListLengthAsync(queueKey)) > 0)
                {
                    var msgV = await redis.GetDatabase().ListRightPopAsync(queueKey);
                    /*
                    var startIndex = 0;
                    var endIndex = count - 1;
                    var msgVList =await redis.GetDatabase().ListRangeAsync(queueKey,startIndex,endIndex);
                    */
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
        public async Task<bool> SendHoursQueueAsync(string queueKeyPrefix, string msg, int expiryHours = 2, CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryHours <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryHours)} must be greater than 1 !");
            }
            return await SendQueueWithKeyExpiryAsync(queueKeyPrefix, msg, KeyExpiryTimeType.Hours, expiryHours,token);
        }

        /// <summary>
        /// 发送消息到队列，使用List实现，有重复
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="expiryHours">过期小时</param>
        /// <returns>返回是否发送成功</returns>
        public async Task<bool> SendMinutesQueueAsync(string queueKeyPrefix, string msg, int expiryMinutes = 2, CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryMinutes <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryMinutes)} must be greater than 1 !");
            }
            return await SendQueueWithKeyExpiryAsync(queueKeyPrefix, msg, KeyExpiryTimeType.Hours, expiryMinutes, token);
        }

        /// <summary>
        /// 接收队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <returns>返回消息列表</returns>
        public async  Task<List<string>> GetHoursMessageAsync(string queueKeyPrefix, int count = 1)
        {
            return await GetMessageByKeyExpiryAsync(queueKeyPrefix, count, KeyExpiryTimeType.Hours);
        }

        /// <summary>
        /// 接收队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <returns>返回消息列表</returns>
        public async  Task<List<string>> GetMinutesMessageAsync(string queueKeyPrefix, int count = 1)
        {
            return await GetMessageByKeyExpiryAsync(queueKeyPrefix, count, KeyExpiryTimeType.Minutes);
        }

        #endregion


        #region 按小时做Key的ZSET排序队列，无重复，整个消息过期是按照Key算，异步方法

        #region 基础方法
        /// <summary>
        /// 发送消息到排序队列，使用ZSET，同一Key内无重复
        /// 排序按照时间戳升序
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="expiryHours">过期小时</param>
        /// <returns>返回是否发送成功</returns>
        private async Task<bool> SendSortQueueWithKeyExpiryAsync(
            string queueKeyPrefix,
            string msg,
            KeyExpiryTimeType keyExpiryTimeType = KeyExpiryTimeType.Hours,
            int expiryTimes = 2,
            CancellationToken token = default)
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
            //lock (lockObj)
            using (await ZSetWriteSyncSemaphore.LockAsync(token))
            {
                RedisKey queueKey = GetQueueKey(keyExpiryTimeType, RedisDataType.ZSet, queueKeyPrefix);
                RedisValue queueItems = msg;
                var score = DateTime.Now.Ticks;
                var when = When.NotExists;
                var keyExists = await redis.GetDatabase().KeyExistsAsync(queueKey);
                var addResult = await redis.GetDatabase().SortedSetAddAsync(queueKey, queueItems, score, when);
                if (!keyExists&& addResult)
                {
                    await redis.GetDatabase().KeyExpireAsync(queueKey, expiry);
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
        private async Task<List<string>> GetSortMessageByKeyExpiryAsync(
            string queueKeyPrefix, 
            int count = 1,
            KeyExpiryTimeType keyExpiryTimeType = KeyExpiryTimeType.Hours,
            CancellationToken token = default)
        {
            bool getBySingle = false;
            var msgList = new List<string>();
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            var queueKeyPattern = GetQueueKeyPattern(keyExpiryTimeType, RedisDataType.ZSet, queueKeyPrefix);

#if NETCOREAPP3_0 || NETCOREAPP3_1
            var keyList =await GetkeysByPrefixAsync(queueKeyPattern);
#else
            var keyList = GetkeysByPrefix(queueKeyPattern);
#endif
            foreach (var item in keyList)
            {
                RedisKey queueKey = item;
                var keyExists = redis.GetDatabase().KeyExists(queueKey);
                if (!keyExists)
                {
                    continue;
                }

                while (msgList.Count < count && (await redis.GetDatabase().SortedSetLengthAsync(queueKey)) > 0)
                {
                    //ZPOPMIN命令需要Redis 5及以上版本SortedSetPop方法
                    //redis.GetDatabase().SortedSetScan(queueKey,, Order.Ascending);                    
                    if (getBySingle)
                    {

                        if (_redisServerVersion < new Version("5.0.0"))
                        {
                            var startIndex = 0;
                            var endIndex = 0;
                            var vList = await redis.GetDatabase().SortedSetRangeByRankAsync(queueKey, startIndex, endIndex, Order.Ascending);
                            if (vList == null || vList.Count() == 0)
                            {
                                break;
                            }
                            var msgV = vList.FirstOrDefault();
                            msgList.Add(msgV);
                            await redis.GetDatabase().SortedSetRemoveAsync(queueKey, msgV);
                        }
                        else
                        {
                            var sortedSetEntry = await redis.GetDatabase().SortedSetPopAsync(queueKey, Order.Ascending);
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

                        if (_redisServerVersion < new Version("5.0.0"))
                        {
                            using (await ZSetReadSyncSemaphore.LockAsync(token))
                            {
                                var startIndex = 0;
                                var endIndex = getCount - 1;
                                var vList = await redis.GetDatabase().SortedSetRangeByRankAsync(queueKey, startIndex, endIndex, Order.Ascending);
                                if (vList == null || vList.Count() == 0)
                                {
                                    break;
                                }
                                await redis.GetDatabase().SortedSetRemoveAsync(queueKey, vList);
                                foreach (var msgV in vList)
                                {
                                    msgList.Add(msgV);
                                    //await redis.GetDatabase().SortedSetRemoveAsync(queueKey, msgV);
                                }
                            }
                        }
                        else
                        {
                            var sortedSetEntryList = await redis.GetDatabase().SortedSetPopAsync(queueKey, getCount, Order.Ascending);
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
        public async Task<bool> SendHoursSortQueueAsync(string queueKeyPrefix, string msg, int expiryHours = 2, CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryHours <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryHours)} must be greater than 1 !");
            }
            return await SendSortQueueWithKeyExpiryAsync(queueKeyPrefix, msg, KeyExpiryTimeType.Hours, expiryHours, token);
        }

        /// <summary>
        /// 发送消息到排序队列，使用ZSET，同一Key内无重复
        /// 排序按照时间戳升序
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="msg">发送的消息</param>
        /// <param name="expiryHours">过期小时</param>
        /// <returns>返回是否发送成功</returns>
        public async Task<bool> SendMinutesSortQueueAsync(string queueKeyPrefix, string msg, int expiryMinutes = 2, CancellationToken token = default)
        {
            if (string.IsNullOrEmpty(msg))
            {
                throw new ArgumentNullException($"please set param {nameof(msg)}!");
            }
            if (expiryMinutes <= 1)
            {
                throw new ArgumentNullException($"param {nameof(expiryMinutes)} must be greater than 1 !");
            }
            return await SendSortQueueWithKeyExpiryAsync(queueKeyPrefix, msg, KeyExpiryTimeType.Minutes, expiryMinutes, token);
        }

        /// <summary>
        /// 接收排序队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <returns>返回消息列表</returns>
        public async Task<List<string>> GetHoursSortMessageAsync(string queueKeyPrefix, int count = 1,CancellationToken token = default)
        {
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            return await GetSortMessageByKeyExpiryAsync(queueKeyPrefix, count, KeyExpiryTimeType.Hours, token);
        }

        /// <summary>
        /// 接收排序队列消息
        /// （过期时间为整个Key内的所有消息过期，从第一个消息的Key开始）
        /// </summary>
        /// <param name="queueKeyPrefix">队列Key前缀</param>
        /// <param name="count">获取消息数量</param>
        /// <returns>返回消息列表</returns>
        public async Task<List<string>> GetMinutesSortMessageAsync(string queueKeyPrefix, int count = 1,CancellationToken token = default)
        {
            if (count <= 0)
            {
                throw new ArgumentNullException($"param {nameof(count)} must be greater than 0 !");
            }
            return await GetSortMessageByKeyExpiryAsync(queueKeyPrefix, count, KeyExpiryTimeType.Minutes, token);
        }
        #endregion


        #region private method

        private async Task<List<string>> GetkeysByPrefixAsync(string queueKeyPrefix)
        {
            List<string> keyList = new List<string>();
            foreach (var server in _serverList)
            {
                var keys = server.KeysAsync(_redisQueueOptions.DefaultDatabase, $"{queueKeyPrefix}*");
                await foreach (var item in keys)
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
