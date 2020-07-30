using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace TZ.RedisQueue.Configurations
{
    public static class RedisQueueServiceCollectionExtensions
    {
        public static IServiceCollection AddRedisQueue(this IServiceCollection services,int defaultDatabase,string host,int port,string password)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }
            services.AddOptions();
            services.Configure<RedisQueueOptions>(configureOptions =>
            {
                configureOptions.DefaultDatabase = defaultDatabase;
                configureOptions.Host = host;
                configureOptions.Port = port;
                configureOptions.Password = password;
            });
            return services;
        }

        public static IServiceCollection AddRedisQueue(this IServiceCollection services, 
            IConfiguration configuration = null,
            string sectionName=nameof(RedisQueueOptions))
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }
            if (configuration == null)
            {
                //在当前目录或者根目录中寻找appsettings.json文件
                var fileName = "appsettings.json";

                var directory = AppContext.BaseDirectory;
                directory = directory.Replace("\\", "/");

                var filePath = $"{directory}/{fileName}";
                if (!File.Exists(filePath))
                {
                    var length = directory.IndexOf("/bin");
                    filePath = $"{directory.Substring(0, length)}/{fileName}";
                }

                var builder = new ConfigurationBuilder()
                    .AddJsonFile(filePath, false, true);

                configuration = builder.Build();
            }
            services.AddOptions();
            services.Configure<RedisQueueOptions>(configuration.GetSection(sectionName));
            return services;
        }
    }
}