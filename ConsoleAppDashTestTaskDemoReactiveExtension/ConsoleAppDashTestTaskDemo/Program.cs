using MoreLinq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    class Program
    {
        internal sealed class MarketDataUpdate
        {
            public string InstrumentId { get; set; }

            public Dictionary<byte, long> Fields { get; set; }

            public DateTime Timestamp { get; set; }

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

        /*private static dynamic ProcessingMarketDataStream(IList<MarketDataUpdate> data)
        {
            data.Reverse();

            var result = data.GroupBy(c => c.InstrumentId).Select(g => new {
                InstrumentId = g.Key,
                Data = g.SelectMany(p => p.Fields.Reverse()
                        .Select(f => new { FieldNo = f.Key, FieldVal = f.Value }))
                        .DistinctBy(c => c.FieldNo).Reverse().ToList()
             }).OrderBy(c => c.InstrumentId).ToList();

            return result;
        }
        private static void PrintData(dynamic data)
        {
            foreach (var current in data)
            {
                Console.WriteLine($"InstrumentId : {current.InstrumentId}");
                foreach (var c in current.Data)
                {

                    Console.WriteLine($"FieldNo - {c.FieldNo}, FieldVal - {c.FieldVal}");
                }
            }
        }
        */

        private static IList<MarketDataUpdate> ProcessingMarketDataStream(IList<MarketDataUpdate> data)
        {
            var result = data.OrderBy(c => c.Timestamp).Reverse().GroupBy(c => c.InstrumentId).Select(g => {

                var currentResult = new MarketDataUpdate 
                { 
                    InstrumentId = g.Key,
                    Timestamp = DateTime.Now,
                    Fields = new Dictionary<byte, long>() 
                };

                g.SelectMany(p => p.Fields.Reverse()
                        .Select(f => new { FieldNo = f.Key, FieldVal = f.Value }))
                        .DistinctBy(c => c.FieldNo)
                        .Reverse()
                        .ForEach(c => currentResult.Fields.Add(c.FieldNo, c.FieldVal));

                return currentResult;

            })
            .OrderBy(c => c.InstrumentId)
            .ToList();

            return result;
        }

        private static void Print(IList<MarketDataUpdate> data)
        {
            foreach (var current in data)
            {
                Console.WriteLine($"{current}");
            }
        }

        public static void Start(ISubject<MarketDataUpdate> source)
        {
            var _randomizer = new Random();

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
                    source.OnNext(update);
                }
            });
        }

        static void Main()
        {
            var marketData = new List<MarketDataUpdate>{
                 new MarketDataUpdate() { InstrumentId = "AAPL_1", Timestamp=DateTime.Now, Fields = new Dictionary<byte, long>() {
                                                                            { 1, 10 },
                                                                            { 4, 200 },
                                                                            { 12, 187 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_2", Timestamp=DateTime.Now, Fields = new Dictionary<byte, long>() {
                                                                            { 1, 12 },
                                                                            { 4, 210 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1", Timestamp=DateTime.Now, Fields = new Dictionary<byte, long>() {
                                                                            { 12, 189 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1", Timestamp=DateTime.Now, Fields = new Dictionary<byte, long>() {
                                                                            { 2, 24 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_2", Timestamp=DateTime.Now, Fields = new Dictionary<byte, long>() {
                                                                            { 5, 120 }
                                                                          }
                                      }
            };

            var data = ProcessingMarketDataStream(marketData);
            Console.WriteLine("Test task control data.");
            Print(data);
            Console.WriteLine("-------------------------------------------------------------------------------");

            // 4 per second - 1000/4, 5 per second - 1000/5

            var source = new Subject<MarketDataUpdate>();

            // накапливаем данные в буфере в течении 250 ms. - агрегация.
            // источников может быть несколько Concat, Merge RX.
            var result = source.AsObservable().Buffer(TimeSpan.FromMilliseconds(1000 / 4));

            result.Subscribe(x =>
            {
                Console.WriteLine($"Buffered Count - {x.Count}");

                var stopWatch = Stopwatch.StartNew();

                // Do work - Processing aggregated data.
                var data = ProcessingMarketDataStream(x);

                stopWatch.Stop();
                Console.WriteLine($"Time elapsed: {stopWatch.Elapsed} TotalMilliseconds : {stopWatch.Elapsed.TotalMilliseconds.ToString("0.0###")}");

                Console.WriteLine($"Agregated Count - {data.Count}");
                Console.WriteLine("Current Agregated Data Result:");

                Print(data);

                Console.WriteLine("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            },
            () => Console.WriteLine("Completed"));


            IObservable<long> observable = Observable.Interval(TimeSpan.FromMilliseconds(1), Scheduler.Immediate);
            //IObservable<long> observable = Observable.Interval(TimeSpan.FromMilliseconds(1), Scheduler.CurrentThread);
            

            var _randomizer = new Random();
            using (observable.Subscribe(x => {

                var update = new MarketDataUpdate();
                update.InstrumentId = "AAPL_" + _randomizer.Next(1, 4);
                update.Fields = new Dictionary<byte, long>();
                for (int i = 0; i < _randomizer.Next(1, 5); i++)
                {
                    update.Fields[(byte)_randomizer.Next(1, 6)] = _randomizer.Next(1, 10000);
                }

                source.OnNext(update);
            }))
            {
                Console.WriteLine("Press Enter to unsubscribe.");
                Console.ReadLine();
            }

            //Start(source);
            Console.ReadLine();
        }
    }
}
