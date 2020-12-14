using System;

namespace MarketDataAggregator
{
    class Program
    {
        static void Main(string[] args)
        {
            var stream1 = new MarketDataStream();
            var stream2 = new MarketDataStream();

            var aggregator = new ThrottledMarketDataStream();

            stream1.AddWatcher(aggregator);
            stream2.AddWatcher(aggregator);

            var client = new Client();

            aggregator.AddWatcher(client);

            stream1.Start();
            stream2.Start();

            Console.ReadLine();
        }
    }
}
