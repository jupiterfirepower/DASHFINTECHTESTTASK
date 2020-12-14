using MarketDataThrottling;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataAggregator
{
    class Program
    {
        static void Main()
        {
            TaskScheduler.UnobservedTaskException +=
                        (object sender, UnobservedTaskExceptionEventArgs e) =>
                        {
                            e.SetObserved();
                            e.Exception.Handle(ex =>
                            {
                                Console.WriteLine($"Exception Type: {ex.GetType()}, Message - {ex.Message}");
                                return true;
                            });
                        };

            try
            {
                var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .Build();

                var appConfig = builder.GetSection("DataAggregatorSettings").Get<AppSettings>();

                DataAggregatorAppSettings.SetAppSettings(appConfig);

                var stream1 = new MarketDataStream();
                var stream2 = new MarketDataStream();

                var aggregator = new ThrottledMarketDataStream();
                aggregator.Start();

                stream1.AddWatcher(aggregator);
                stream2.AddWatcher(aggregator);

                var client = new Client();

                aggregator.AddWatcher(client);

                stream1.Start();
                stream2.Start();
                Thread.Sleep(5000);
                aggregator.End();
                Thread.Sleep(3000);
                stream1.End();
                stream2.End();

                aggregator.Start();
                stream1.Start();
                stream2.Start();
                Thread.Sleep(5000);
                aggregator.End();
                Thread.Sleep(2000);
                stream1.End();
                stream2.End();


                aggregator.Start();
                stream1.Start();
                stream2.Start();
                Thread.Sleep(5000);
                aggregator.End();
                Thread.Sleep(2000);
                stream1.End();
                stream2.End();

                Console.WriteLine("Press Enter to exit.");
                Console.ReadLine();
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }
    }
}
