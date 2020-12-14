using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MarketDataAggregator.Helpers;
using MarketDataAggregator.DomainModel;
using System.Runtime.CompilerServices;
using MarketDataThrottling.Contracts;
using System.Reactive.Disposables;
using System.Diagnostics;

[assembly: InternalsVisibleTo("UnitTestMarketDataThrottling")]
namespace MarketDataAggregator
{

    internal class Client : IAggreagatedMarketDataObserver
    {
        public void OnUpdate(MarketDataUpdate[] marketDataUpdates)
        {
            foreach (var marketDataUpdate in marketDataUpdates)
            {
                Console.WriteLine("Client - {0}", marketDataUpdate);
            }
        }
    }

    internal class ThrottledMarketDataStream : IMarketDataObserver, IThrottledMarketDataStream
    {
        private readonly IList<IAggreagatedMarketDataObserver> _watchers = new List<IAggreagatedMarketDataObserver>();
        private readonly Lazy<DataAggregator> _aggregator = null;

        private readonly object _locker = new object();
        private readonly object _lockerAggregator = new object();
        private DataAggregator aggregator => _aggregator.Value;

        public int WatcherCount => _watchers.Count;

        public ThrottledMarketDataStream()
        {
            _aggregator = new Lazy<DataAggregator>(() => new DataAggregator(_watchers), true);
        }

        public void AddWatcher(IAggreagatedMarketDataObserver watcher)
        {
            lock(_locker)
            {
                _watchers.Add(watcher);
            }
        }

        public void RemoveWatcher(IAggreagatedMarketDataObserver watcher)
        {
            lock (_locker)
            {
                _watchers.Remove(watcher);
            }
        }

        public void Start()
        {
            lock (_lockerAggregator)
            {
                aggregator.Start();
            }
        }

        public void End()
        {
            lock (_lockerAggregator)
            {
                AsyncHelper.RunSync(() => aggregator.CompleteAsync());
            }
        }

        public void OnUpdate(MarketDataUpdate marketDataUpdate)
        {
            Task.Run(async () => await aggregator.SendDataAsync(marketDataUpdate));
        }
    }

    internal class BasicObservable<T> : IObservable<T>
    {
        List<IObserver<T>> _observers = new List<IObserver<T>>();

        public BasicObservable(
            Func<T> getData,
            TimeSpan? interval = null,
            CancellationToken cancellationToken = default
            ) =>

            Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(interval ?? TimeSpan.FromMilliseconds(1));
                        _observers.ForEach(o => o.OnNext(getData()));
                    }
                    catch (Exception ex)
                    {
                        _observers.ForEach(o => o.OnError(ex));
                    }
                }

                _observers.ForEach(o => o.OnCompleted());

            }, cancellationToken);

        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observers.Add(observer);
            return Disposable.Create(observer, (o) => _observers.Remove(o));
        }
    }

    class Subscriber : IDisposable
    {
        public string Name;
        private IDisposable _disposable;
        private IMarketDataObserver _marketDataProcessor;

        //Listen for OnNext and write to the debug window when it happens
        public Subscriber(IObservable<MarketDataUpdate> observable, string name, IMarketDataObserver marketDataProcessor)
        {
            Name = name;
            _marketDataProcessor = marketDataProcessor;
            _disposable = observable.Subscribe((s) => { 
                                                        //Console.WriteLine($"Subscriber Name: {Name} Message: {s}");
                                                        _marketDataProcessor.OnUpdate(s);
                                                      }); 
        }

        public void Dispose() => _disposable.Dispose();

    }


    internal class MarketDataStream : IMarketDataStream
    {
        private readonly List<IMarketDataObserver> _watchers = new List<IMarketDataObserver>();
        private readonly List<Subscriber> _subscribers = new List<Subscriber>();
        private readonly Random _randomizer = new Random(new Random().Next(1, int.MaxValue));
        private CancellationTokenSource _ts = new CancellationTokenSource();

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
            try
            {
                _ts.Cancel();
                Thread.Sleep(5);
                using (_ts)
                {
                    _subscribers.ForEach(s => { using (s) { }; });
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Error : {ex.Message}");
            }
            finally
            {
                _ts = new CancellationTokenSource();
            }
        }

        private MarketDataUpdate getMarketData()
        {
            var update = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_" + _randomizer.Next(1, 100),
                Fields = new Dictionary<byte, long>()
            };

            for (int i = 0; i < _randomizer.Next(1, 5); i++)
            {
                update.Fields[(byte)_randomizer.Next(1, 20)] = _randomizer.Next(1, 10000);
            }

            return update;
        }

        public void Start()
        {
            var publisher = new BasicObservable<MarketDataUpdate>(getMarketData, default, _ts.Token);

            for(int i = 0; i < _watchers.Count; i++ )
            {
                _subscribers.Add(new Subscriber(publisher, i.ToString(), _watchers[i]));
            }
        }


        /*public void Start()
        {
            Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(1);

                    if (_ts.IsCancellationRequested)
                    {
                        Console.WriteLine("task canceled.");
                        break;
                    }

                    var update = new MarketDataUpdate()
                    {
                        InstrumentId = "AAPL_" + _randomizer.Next(1, 100),
                        Fields = new Dictionary<byte, long>()
                    };

                    for (int i = 0; i < _randomizer.Next(1, 5); i++)
                    {
                        update.Fields[(byte)_randomizer.Next(1, 20)] = _randomizer.Next(1, 10000);
                    }

                    foreach (var watcher in _watchers)
                    {
                        watcher.OnUpdate(update);
                    }
                }
            }, _ts.Token);
    }*/
    }
}
