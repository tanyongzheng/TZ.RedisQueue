using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;
using TZ.RedisQueue.Configurations;

namespace Demo
{
    public class RedisQueueConfigOptions : IOptions<RedisQueueOptions>
	{
		private RedisQueueOptions RedisQueueOptions;
		public RedisQueueOptions Value
		{
			get
			{
				return RedisQueueOptions;
			}
		}

		public void SetConfig()
		{
			RedisQueueOptions = new RedisQueueOptions();
			RedisQueueOptions.DefaultDatabase = 2;
			RedisQueueOptions.Host = "127.0.0.1";
			RedisQueueOptions.Port = 6379;
			//RedisQueueOptions.RedisVersion = "3.2.100";
			//RedisQueueOptions.Password = null;
		}
	}
}