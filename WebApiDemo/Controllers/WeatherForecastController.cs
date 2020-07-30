using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using TZ.RedisQueue;

namespace WebApiDemo.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly RedisQueueService _redisQueueService;

        public WeatherForecastController(ILogger<WeatherForecastController> logger,
            RedisQueueService redisQueueService)
        {
            _logger = logger;
            _redisQueueService = redisQueueService;
        }

        [HttpGet]
        public async  Task<IEnumerable<WeatherForecast>> Get()
        {

            var queueKeyPrefix = "AsyncCallApi";
            for (var i = 0; i < 1000; i++)
            {
                var result = await _redisQueueService.SendHoursQueueAsync(queueKeyPrefix, "A" + i);
                if (!result)
                {
                    //Console.WriteLine("A" + i + "入队失败");
                }
            }
            //.SendHoursQueue()
            var rng = new Random();
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = rng.Next(-20, 55),
                Summary = Summaries[rng.Next(Summaries.Length)]
            })
            .ToArray();
        }
    }
}
