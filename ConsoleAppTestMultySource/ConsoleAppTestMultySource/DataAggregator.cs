using System;
using System.Threading.Tasks.Dataflow;

namespace ConsoleApp3
{
    using ConsoleApp3.DomainModel;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    public class DataAggregator
    {
        //private BatchBlock<MarketDataUpdate> batchBlock = new BatchBlock<MarketDataUpdate>(5, new GroupingDataflowBlockOptions() { Greedy = false });
        private BatchBlock<MarketDataUpdate> batchBlock = new BatchBlock<MarketDataUpdate>(batchSize: 5, new GroupingDataflowBlockOptions() { Greedy = true });
        private TransformBlock<MarketDataUpdate[], List<MarketDataUpdate>> transformBlock1 = new TransformBlock<MarketDataUpdate[], List<MarketDataUpdate>>(e =>
        {
            var data = e.OrderBy(x => x.Timestamp);
            var grouped = data.ToLookup(p => p.InstrumentId);

            var result = new List<MarketDataUpdate>();

            foreach (var dataGroup in grouped)
            {
                var currentResult = new MarketDataUpdate
                {
                    InstrumentId = dataGroup.Key,
                    Fields = new Dictionary<byte, long>(),
                    Timestamp = DateTime.Now
                };

                var current = dataGroup.SelectMany(c => c.Fields).ToList();
                current.ForEach(x => currentResult.Fields[x.Key] = x.Value);
                result.Add(currentResult);
            }

            return result.OrderBy(c => c.InstrumentId).ToList();
        });

        private TransformBlock<MarketDataUpdate[], List<MarketDataUpdate>> transformBlock2 = new TransformBlock<MarketDataUpdate[], List<MarketDataUpdate>>(e =>
        {
            return e.OrderBy(x => x.Timestamp)
                            .GroupBy(
                                x => new { x.InstrumentId },
                                (x, y) => new MarketDataUpdate
                                {
                                    InstrumentId = x.InstrumentId,
                                    Timestamp = DateTime.Now,
                                    Fields = y
                                            .SelectMany(z => z.Fields)
                                            .ToLookup(z => z.Key, z => z.Value)
                                            .ToDictionary(z => z.Key, z => z.Last())
                                }).OrderBy(c => c.InstrumentId).ToList();
        });

        private ActionBlock<List<MarketDataUpdate>> writer = new ActionBlock<List<MarketDataUpdate>>(cd => {
            Console.WriteLine("-------------------------------------------------------");
            cd.ForEach(d => Console.WriteLine(d));
            Console.WriteLine("*******************************************************");
        });
        /*,
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                SingleProducerConstrained = true
            }*/

        private BufferBlock<MarketDataUpdate> source1 = new BufferBlock<MarketDataUpdate>(new DataflowBlockOptions() { BoundedCapacity = 100 });
        private BufferBlock<MarketDataUpdate> source2 = new BufferBlock<MarketDataUpdate>(new DataflowBlockOptions() { BoundedCapacity = 100 });
        private BufferBlock<MarketDataUpdate> source3 = new BufferBlock<MarketDataUpdate>(new DataflowBlockOptions() { BoundedCapacity = 100 });
        private BufferBlock<MarketDataUpdate> source4 = new BufferBlock<MarketDataUpdate>(new DataflowBlockOptions() { BoundedCapacity = 100 });
        private BufferBlock<MarketDataUpdate> source5 = new BufferBlock<MarketDataUpdate>(new DataflowBlockOptions() { BoundedCapacity = 100 });

        public DataAggregator()
        {
            source1.LinkTo(batchBlock, new DataflowLinkOptions() { PropagateCompletion = true }); 
            source2.LinkTo(batchBlock, new DataflowLinkOptions() { PropagateCompletion = true }); 
            source3.LinkTo(batchBlock, new DataflowLinkOptions() { PropagateCompletion = true }); 
            source4.LinkTo(batchBlock, new DataflowLinkOptions() { PropagateCompletion = true }); 
            source5.LinkTo(batchBlock, new DataflowLinkOptions() { PropagateCompletion = true }); 

            //batchBlock.LinkTo(transformBlock1, new DataflowLinkOptions() { PropagateCompletion = true });
            //transformBlock1.LinkTo(writer, new DataflowLinkOptions() { PropagateCompletion = true });


            var broadcastBlock = new BroadcastBlock<MarketDataUpdate[]>(null);

            new Timer(x => { 
                batchBlock.TriggerBatch(); 
            }).Change(1000, 200);

            batchBlock.LinkTo(broadcastBlock); //{ PropagateCompletion = true }


            broadcastBlock.LinkTo(transformBlock1);
            broadcastBlock.LinkTo(transformBlock2);
            var writeOnceBlock = new WriteOnceBlock<List<MarketDataUpdate>>(null);
            transformBlock1.LinkTo(writeOnceBlock);
            transformBlock2.LinkTo(writeOnceBlock);

            writeOnceBlock.LinkTo(writer, new DataflowLinkOptions() { PropagateCompletion = true }); 
        }

        public void TestPipeline()
        {
            var tdata1 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_1",
                Fields = new Dictionary<byte, long>() {
                                                         { 1, 10 },
                                                         { 4, 200 },
                                                         { 12, 187 }
                                                      },
                Timestamp = DateTime.Now
            };
            source1.Post(tdata1);

            var tdata2 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_2",
                Fields = new Dictionary<byte, long>() {
                                                         { 1, 12 },
                                                         { 4, 210 }
                                                      },
                Timestamp = DateTime.Now
            };

            source2.Post(tdata2);

            var tdata3 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_1",
                Fields = new Dictionary<byte, long>() {
                                                         { 12, 189 }
                                                      },
                Timestamp = DateTime.Now
            };

            source3.Post(tdata3);

            var tdata4 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_1",
                Fields = new Dictionary<byte, long>() {
                                                             { 2, 24 }
                                                           },
                Timestamp = DateTime.Now
            };
            source4.Post(tdata4);

            var tdata5 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_2",
                Fields = new Dictionary<byte, long>() {
                                                        { 5, 120 }
                                                      }
                ,
                Timestamp = DateTime.Now
            };
            source5.Post(tdata5);

            var tdata6 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_3",
                Fields = new Dictionary<byte, long>() {
                                                        { 22, 121 }
                                                      }
            };
            source1.Post(tdata6);


            var tdata7 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_4",
                Fields = new Dictionary<byte, long>() {
                                                        { 15, 122 }
                                                      }
            };
            source2.Post(tdata7);

            var tdata8 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_5",
                Fields = new Dictionary<byte, long>() {
                                                        { 17, 124 }
                                                      }
            };
            source3.Post(tdata8);

            var tdata9 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_6",
                Fields = new Dictionary<byte, long>() {
                                                        { 21, 125 }
                                                      }
            };
            source4.Post(tdata9);

            var tdata10 = new MarketDataUpdate()
            {
                InstrumentId = "AAPL_7",
                Fields = new Dictionary<byte, long>() {
                                                        { 24, 127 }
                                                      }
            };
            source5.Post(tdata10);

            source1.Complete();
            source2.Complete();
            source3.Complete();
            source4.Complete();
            source5.Complete();
            writer.Completion.Wait();
            Console.ReadLine();
        }
    }
}
