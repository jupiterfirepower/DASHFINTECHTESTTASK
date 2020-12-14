using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataAggregator
{
    internal sealed class MarketDataUpdate
    {
        public string InstrumentId { get; set; }

        public Dictionary<byte, long> Fields { get; set; }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.Append($"Instrument: {InstrumentId}, FieldsNo: {Fields.Count}, Fields: [");
            foreach (var field in Fields)
                builder.Append($"{field.Key}: {field.Value}, ");
            builder.Append($"]");
            return builder.ToString();
        }
    }

    internal interface IMarketDataObserver
    {
        void OnUpdate(MarketDataUpdate marketDataUpdate);
    }

    internal interface IAggreagatedMarketDataObserver
    {
        void OnUpdate(MarketDataUpdate[] marketDataUpdate);
    }

    internal interface IMarketDataStream
    {
        void AddWatcher(IMarketDataObserver watcher);

        void RemoveWatcher(IMarketDataObserver watcher);

        void Start();

        void End();
    }

    internal interface IThrottledMarketDataStream
    {
        void AddWatcher(IAggreagatedMarketDataObserver watcher);

        void RemoveWatcher(IAggreagatedMarketDataObserver watcher);

        void Start();

        void End();
    }

    internal class Client : IAggreagatedMarketDataObserver
    {
        public void OnUpdate(MarketDataUpdate[] marketDataUpdates)
        {
            foreach (var marketDataUpdate in marketDataUpdates)
            {
                Console.WriteLine(marketDataUpdate);
            }
        }
    }

    internal class ThrottledMarketDataStream : IMarketDataObserver, IThrottledMarketDataStream
    {
        private readonly List<IAggreagatedMarketDataObserver> _watchers = new List<IAggreagatedMarketDataObserver>();

        public void AddWatcher(IAggreagatedMarketDataObserver watcher)
        {
            _watchers.Add(watcher);
        }

        public void RemoveWatcher(IAggreagatedMarketDataObserver watcher)
        {
            _watchers.Remove(watcher);
        }

        public void Start()
        {
            throw new NotImplementedException();
        }

        public void End()
        {
            throw new NotImplementedException();
        }

        public void OnUpdate(MarketDataUpdate marketDataUpdate)
        {
            foreach (var watcher in _watchers)
            {
                watcher.OnUpdate(new[] { marketDataUpdate });
            }
        }
    }

    internal class MarketDataStream : IMarketDataStream
    {
        private readonly List<IMarketDataObserver> _watchers = new List<IMarketDataObserver>();
        private Random _randomizer = new Random();

        public void AddWatcher(IMarketDataObserver watcher)
        {
            _watchers.Add(watcher);
        }

        public void RemoveWatcher(IMarketDataObserver watcher)
        {
            _watchers.Remove(watcher);
        }

        public void End()
        {
            throw new NotImplementedException();
        }

        public void Start()
        {
            Task.Run(() =>
                {
                    while (true)
                    {
                        Thread.Sleep(1);
                        var update = new MarketDataUpdate();
                        update.InstrumentId = "AAPL_" + _randomizer.Next(1, 100);
                        update.Fields = new Dictionary<byte, long>();
                        for (int i = 0; i < _randomizer.Next(1, 5); i++)
                        {
                            update.Fields[(byte)_randomizer.Next(1, 20)] = _randomizer.Next(1, 10000);
                        }

                        foreach (var watcher in _watchers)
                        {
                            watcher.OnUpdate(update);
                        }
                    }
                });
        }
    }
}
