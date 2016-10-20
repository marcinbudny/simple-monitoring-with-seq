using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace ConsoleApplication
{
    public class Program
    {
        const int MinProcessingTimeInMs = 100;
        const int MaxProcessingTimeInMs = 500;
        const int ProduceSpeedCycleLengthInMs = 50000;

        static readonly DateTime _startedAt = DateTime.Now; 
       
        public static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Seq("http://localhost:5341")
                .WriteTo.ColoredConsole()
                .CreateLogger();

            var cts = new CancellationTokenSource();
            var queue = new BlockingCollection<int>();

            var tasks = new[] 
            {
                Task.Run(() => ConsumeData(queue, cts.Token)),
                //Task.Run(() => ConsumeData(queue, cts.Token)),
                Task.Run(() => ProduceData(queue, cts.Token)),
                Task.Run(() => ObserveQueueLength(queue, cts.Token))
            };
            
            Console.ReadLine();

            cts.Cancel();
            Task.WaitAll(tasks.ToArray());

            Log.Information("Processing completed");

            Log.CloseAndFlush();
        }


        static async Task ProduceData(BlockingCollection<int> queue, CancellationToken token) 
        {
            var i = 0;
            try
            {
                while(!token.IsCancellationRequested)
                {
                    var producingTime = GetCurrentDelay();
                    await Task.Delay(producingTime, token);

                    Log.Debug("Event {EventName}, item was {Item}, duration was {DurationInMs} ms", 
                        "ItemProduced", i, producingTime.TotalMilliseconds);

                    queue.Add(i++);
                }
            }
            catch(TaskCanceledException) {}

            queue.CompleteAdding();
        }

       static TimeSpan GetCurrentDelay() 
       {
           var elapsed = (DateTime.Now - _startedAt).TotalMilliseconds;
           var scaled = (elapsed / ProduceSpeedCycleLengthInMs) * 2 * Math.PI; 
           var cycle = Math.Cos(scaled);
           
           var processingTimeVariance = (MaxProcessingTimeInMs - MinProcessingTimeInMs) / 2;
           var meanProcessingTime = (MaxProcessingTimeInMs + MinProcessingTimeInMs) / 2;

           var currentDelay = (double)meanProcessingTime + cycle * (double)processingTimeVariance;
           return TimeSpan.FromMilliseconds(currentDelay);
       }


        static async Task ConsumeData(BlockingCollection<int> queue, CancellationToken token) 
        {
            try
            {
                var random = new Random();

                foreach(var item in queue.GetConsumingEnumerable(token)) 
                {
                    var consumingTime = random.Next(MinProcessingTimeInMs, MaxProcessingTimeInMs);
                    
                    await Task.Delay(consumingTime, token);

                    Log.Debug("Event {EventName}, item was {Item}, duration was {DurationInMs} ms", 
                        "ItemConsumed", item, consumingTime);
                }
            }
            catch (TaskCanceledException) { } 
            catch (OperationCanceledException) { } 
        }        

        static async Task ObserveQueueLength(BlockingCollection<int> queue, CancellationToken token)
        {
            try {
                while(!token.IsCancellationRequested)
                {
                    await Task.Delay(500, token);
                    Log.Information("Metric {MetricName} = {MetricValue}", "QueueLength", queue.Count);
                }
            }
            catch (TaskCanceledException) { }
        }

    }
            
}
