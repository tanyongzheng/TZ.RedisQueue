using System;
using System.Collections.Generic;
using System.Text;

namespace TZ.RedisQueue.Configurations
{
    public class RedisQueueOptions
    {        
        public int DefaultDatabase { get; set; }

        public string Host { get; set; }

        public int Port { get; set; }

        public string Password { get; set; }

        //public string RedisVersion { get; set; }

        public string HoursFormatKeySuffix { get; set; }

        public string MinutesFormatKeySuffix { get; set; }
    }

}
