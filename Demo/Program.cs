using System;
using System.Threading.Tasks;
using TZ.RedisQueue;

namespace Demo
{
    class Program
    {

        private static RedisQueueConfigOptions options = new RedisQueueConfigOptions();

        private static RedisQueueService RedisQueueService;
        static async Task Main(string[] args)
        {
            options.SetConfig();
            RedisQueueService = new RedisQueueService(options);
            //SendQueueTest();
            //GetMessageTest();
            //SendSortQueueTest();
            //GetSortMessageTest();
            //await SendQueueTestAsync();
            //await GetMessageTestAsync();
            //await SendSortQueueTestAsync();
            //await GetSortMessageTestAsync();
            //await SendDaysSortQueueTestAsync();
            //await GetDaysSortMessageTestAsync();
            await SendDaysQueueTestAsync();
            //await GetDaysMessageTestAsync();
            //InsertAndGetTest();
            Console.WriteLine("Hello World!");
            Console.ReadKey();
        }

        #region List
        private static void SendQueueTest()
        {
            var queueKeyPrefix = "CallApi";
            for (var i = 0; i < 100000; i++)
            {
                RedisQueueService.SendHoursQueue(queueKeyPrefix, "A" + i);
            }
        }
        private static void GetMessageTest()
        {
            var queueKeyPrefix = "CallApi";
            for (var i = 0; i < 100; i++)
            {
                var list = RedisQueueService.GetHoursMessage(queueKeyPrefix, 1000);
                if (list == null || list.Count == 0)
                {
                    return;
                }
                foreach (var item in list)
                {
                    Console.WriteLine(item);
                }
            }
        } 
        #endregion

        #region List 异步
        private static async Task SendDaysQueueTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            for (var i = 0; i < 50000; i++)
            {
                var result= await RedisQueueService.SendDaysQueueAsync(queueKeyPrefix, "A" + i);
                if (!result)
                {
                    Console.WriteLine("A" + i+"入队失败");
                }
            }
        }
        private static async Task SendQueueTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            for (var i = 0; i < 50000; i++)
            {
                var result= await RedisQueueService.SendHoursQueueAsync(queueKeyPrefix, "A" + i);
                if (!result)
                {
                    Console.WriteLine("A" + i+"入队失败");
                }
            }
        }
        private static async Task GetDaysMessageTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            for (var i = 0; i < 50; i++)
            {
                var list = await RedisQueueService.GetDaysMessageAsync(queueKeyPrefix, 1100);
                if (list == null || list.Count == 0)
                {
                    return;
                }
                foreach (var item in list)
                {
                    Console.Write(item);
                }
            }
        } 
        private static async Task GetMessageTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            for (var i = 0; i < 50; i++)
            {
                var list = await RedisQueueService.GetHoursMessageAsync(queueKeyPrefix, 1100);
                if (list == null || list.Count == 0)
                {
                    return;
                }
                foreach (var item in list)
                {
                    Console.Write(item);
                }
            }
        } 
        #endregion


        #region ZSet
        private static void SendSortQueueTest()
        {
            var queueKeyPrefix = "CallApi";
            for (var i = 0; i < 100; i++)
            {
                RedisQueueService.SendHoursSortQueue(queueKeyPrefix, "A" + i);
            }
            for (var i = 0; i < 100000; i++)
            {
                RedisQueueService.SendHoursSortQueue(queueKeyPrefix, "A" + i);
            }
        }
        private static void GetSortMessageTest()
        {
            var queueKeyPrefix = "CallApi";
            for (var i = 0; i < 100; i++)
            {
                var list = RedisQueueService.GetHoursSortMessage(queueKeyPrefix, 1000);
                if (list == null || list.Count == 0)
                {
                    return;
                }
                foreach (var item in list)
                {
                    Console.WriteLine(item);
                }
            }
        } 
        #endregion

        #region ZSet 异步
        private static async Task SendDaysSortQueueTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            /*
            for (var i = 0; i < 100; i++)
            {
                await RedisQueueService.SendHoursSortQueueAsync(queueKeyPrefix, "A" + i);
            }
            for (var i = 99; i >=0; i--)
            {
                await RedisQueueService.SendHoursSortQueueAsync(queueKeyPrefix, "A" + i);
            }*/
            
            for (var i = 0; i < 100000; i++)
            {
                await RedisQueueService.SendDaysSortQueueAsync(queueKeyPrefix, "A" + i);
            }
        }
        
        private static async Task SendSortQueueTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            /*
            for (var i = 0; i < 100; i++)
            {
                await RedisQueueService.SendHoursSortQueueAsync(queueKeyPrefix, "A" + i);
            }
            for (var i = 99; i >=0; i--)
            {
                await RedisQueueService.SendHoursSortQueueAsync(queueKeyPrefix, "A" + i);
            }*/
            
            for (var i = 0; i < 100000; i++)
            {
                await RedisQueueService.SendMinutesSortQueueAsync(queueKeyPrefix, "A" + i);
            }
        }
        private static async Task GetDaysSortMessageTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            for (var i = 0; i < 1000; i++)
            {
                var list = await RedisQueueService.GetDaysSortMessageAsync(queueKeyPrefix, 2000);
                if (list == null || list.Count == 0)
                {
                    return;
                }
                foreach (var item in list)
                {
                    Console.Write(item);
                }
            }
        } 
        
        private static async Task GetSortMessageTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            for (var i = 0; i < 1000; i++)
            {
                var list = await RedisQueueService.GetMinutesSortMessageAsync(queueKeyPrefix, 1100);
                if (list == null || list.Count == 0)
                {
                    return;
                }
                foreach (var item in list)
                {
                    Console.Write(item);
                }
            }
        } 

        private static async Task InsertAndGetTestAsync()
        {
            var queueKeyPrefix = "AsyncCallApi";
            await Task.Run(async () => {
                for (var i = 0; i < 100000; i++)
                {
                    await RedisQueueService.SendHoursSortQueueAsync(queueKeyPrefix, "A" + i);
                }
            });

            await Task.Run(async () => {
                for (var i = 0; i < 10000; i++)
                {
                    var list = await RedisQueueService.GetHoursSortMessageAsync(queueKeyPrefix, 10);
                    foreach (var item in list)
                        Console.WriteLine(item);
                }
            });
        }

        private static async Task InsertAndGetTest2Async()
        {
            var queueKeyPrefix = "AsyncCallApi";
            var task1 = new Task(async () =>
            {
                for (var i = 0; i < 100000; i++)
                {
                    await RedisQueueService.SendHoursSortQueueAsync(queueKeyPrefix, "A" + i);
                }
            });

            var task2 = new Task(async () =>
            {
                for (var i = 0; i < 10000; i++)
                {
                    var list = await RedisQueueService.GetHoursSortMessageAsync(queueKeyPrefix, 10);
                    foreach (var item in list)
                        Console.WriteLine(item);
                }
            });

            task1.Start();
            task2.Start();
        }

        private static void InsertAndGetTest()
        {
            var queueKeyPrefix = "AsyncCallApi";
            var task1 = new Task( () =>
            {
                for (var i = 0; i < 10000; i++)
                {
                    RedisQueueService.SendHoursSortQueue(queueKeyPrefix, "A" + i);
                }
            });

            var task2 = new Task(() =>
            {
                for (var i = 0; i < 1000; i++)
                {
                    var list = RedisQueueService.GetHoursSortMessage(queueKeyPrefix, 10);
                    foreach (var item in list)
                        Console.WriteLine(item);
                }
            });
            task1.Start();
            task2.Start();
        }
        #endregion
    }
}
