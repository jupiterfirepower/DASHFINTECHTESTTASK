using Microsoft.VisualStudio.TestTools.UnitTesting;
using MarketDataAggregator;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MarketDataAggregator.DomainModel;
using System;
using MarketDataThrottling.Contracts;

namespace UnitTestMarketDataThrottling
{
    [TestClass]
    public class UnitTestMultithreadingMarketDataThrottling
    {
        private static void ThreadMethodAddWatcher(Client c, IThrottledMarketDataStream stream)
        {
            stream.AddWatcher(c);
            System.Diagnostics.Debug.WriteLine("Thread ManagedThreadId : {0}", Thread.CurrentThread.ManagedThreadId);
        }

        private static void ThreadMethodRemoveWatcher(Client c, IThrottledMarketDataStream stream)
        {
            stream.RemoveWatcher(c);

            System.Diagnostics.Debug.WriteLine("Thread ManagedThreadId : {0}", Thread.CurrentThread.ManagedThreadId);
        }

        private void ThrottledMarketDataStreamMultithreadingAddWatchers(List<Client> list, IThrottledMarketDataStream stream)
        {
            // create a bunch of threads
            List<Thread> threads = new List<Thread>();
            list.ForEach(x => threads.Add(new Thread(() => ThreadMethodAddWatcher(x, stream))));

            // start them
            threads.ForEach(x => x.Start());

            // wait for them to finish
            threads.ForEach(x => x.Join());
        }

        [TestMethod]
        public void ThrottledMarketDataStreamMultithreadingAddWatcherTest()
        {
            var stream = new ThrottledMarketDataStream();
            List<Client> list = new List<Client>();
            list.AddRange(new Client[] { new Client(), new Client(), new Client(), new Client(), new Client() });

            ThrottledMarketDataStreamMultithreadingAddWatchers(list, stream);
            // this will not print untill all threads have completed
            System.Diagnostics.Debug.WriteLine("All threads finished.");
            Assert.IsTrue(stream.WatcherCount == 5);
        }

        

        [TestMethod]
        public void ThrottledMarketDataStreamMultithreadingRemoveWatcherTest()
        {
            var stream = new ThrottledMarketDataStream();
            List<Client> list = new List<Client>();
            list.AddRange(new Client[] { new Client(), new Client(), new Client(), new Client(), new Client(), new Client(), new Client(), new Client(), null });

            ThrottledMarketDataStreamMultithreadingAddWatchers(list, stream);
            Assert.IsTrue(stream.WatcherCount == 9);

            // create a bunch of threads
            List<Thread> threads = new List<Thread>();
            list.ForEach(x => threads.Add(new Thread(() => ThreadMethodRemoveWatcher(x, stream))));

            // start them
            threads.ForEach(x => x.Start());

            // wait for them to finish
            threads.ForEach(x => x.Join());

            // this will not print untill all threads have completed
            System.Diagnostics.Debug.WriteLine("All threads finished.");
            System.Diagnostics.Debug.WriteLine($"WatcherCount - {stream.WatcherCount}");
            Assert.IsTrue(stream.WatcherCount == 0);
        }

        private static async Task ThreadMethodAddWatcherAync(Client c, IThrottledMarketDataStream stream)
        {
            await Task.CompletedTask;
            stream.AddWatcher(c);
            System.Diagnostics.Debug.WriteLine("ThreadMethodAddWatcherAync Thread ManagedThreadId : {0}", Thread.CurrentThread.ManagedThreadId);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingAddWatcherAyncTest()
        {
            var stream = new ThrottledMarketDataStream();

            List<Client> list = new List<Client>();
            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                var currentClient = new Client();
                list.Add(currentClient);
                tasks.Add(new Task(async () => await ThreadMethodAddWatcherAync(currentClient, stream)));
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks add watcher finished.");
            });

            Thread.Sleep(100);
            Assert.IsTrue(stream.WatcherCount == 100);
        }

        private static async Task ThreadMethodRemoveWatcherAync(Client c, IThrottledMarketDataStream stream)
        {
            await Task.CompletedTask;
            stream.RemoveWatcher(c);
            System.Diagnostics.Debug.WriteLine("ThreadMethodRemoveWatcherAync Thread ManagedThreadId : {0}", Thread.CurrentThread.ManagedThreadId);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingAddRemoveWatcherAyncTest()
        {
            var stream = new ThrottledMarketDataStream();

            List<Client> list = new List<Client>();
            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                var currentClient = new Client();
                list.Add(currentClient);
                tasks.Add(new Task(async () => await ThreadMethodAddWatcherAync(currentClient, stream)));
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks add watcher finished.");
            });

            Thread.Sleep(100);
            Assert.IsTrue(stream.WatcherCount == 100);

            tasks.Clear();

            list.ForEach(c => tasks.Add(new Task(async () => await ThreadMethodRemoveWatcherAync(c, stream))));

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks remove watcher finished.");
            });

            Thread.Sleep(100);
            Assert.IsTrue(stream.WatcherCount == 0);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingCombinedAddRemoveWatcherTest()
        {
            var stream = new ThrottledMarketDataStream();

            List<Client> list = new List<Client>();
            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                var currentClient = new Client();
                list.Add(currentClient);
                tasks.Add(new Task(async () => await ThreadMethodAddWatcherAync(currentClient, stream)));
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks add watcher finished.");
            });

            Thread.Sleep(100);
            Assert.IsTrue(stream.WatcherCount == 100);

            tasks.Clear();

            for (int i = 0; i < list.Count - 1; i++)
            {
                if( i%2 ==0 )
                {
                    tasks.Add(new Task(async () => await ThreadMethodAddWatcherAync(new Client(), stream)));
                }
                else
                {
                    tasks.Add(new Task(async () => await ThreadMethodRemoveWatcherAync(list[i], stream)));
                }
            }

            list.ForEach(c => tasks.Add(new Task(async () => await ThreadMethodRemoveWatcherAync(c, stream))));

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks add/remove watcher finished.");
            });

            Thread.Sleep(100);
            Assert.IsTrue(stream.WatcherCount == 50);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingOnUpdateTest()
        {
            var stream = new ThrottledMarketDataStream();

            var randomizer = new Random(new Random().Next(1, int.MaxValue));
            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                tasks.Add(new Task(() => 
                {
                    var update = new MarketDataUpdate()
                    {
                        InstrumentId = "AAPL_" + randomizer.Next(1, 100),
                        Fields = new Dictionary<byte, long>(),
                    };

                    for (int i = 0; i < randomizer.Next(1, 5); i++)
                    {
                        update.Fields[(byte)randomizer.Next(1, 20)] = randomizer.Next(1, 10000);
                    }

                    stream.OnUpdate(update);
                }));
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks OnUpdate finished.");
            });

            tasks.Clear();
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingOnUpdateSecondTest()
        {
            var stream = new ThrottledMarketDataStream();

            var randomizer = new Random(new Random().Next(1, int.MaxValue));
            var tasks = new List<Task>();
            List<MarketDataUpdate> list = new List<MarketDataUpdate>();

            for (int i = 0; i < 1000; i++)
            {
                var update = new MarketDataUpdate()
                {
                    InstrumentId = "AAPL_" + randomizer.Next(1, 100),
                    Fields = new Dictionary<byte, long>(),
                };

                for (int j = 0; j < randomizer.Next(1, 5); j++)
                {
                    update.Fields[(byte)randomizer.Next(1, 20)] = randomizer.Next(1, 10000);
                }
                list.Add(update);
            }

            for (int i = 0; i < list.Count - 1; i++)
            {
                tasks.Add(new Task(() =>stream.OnUpdate(list[i]) ));
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks OnUpdate finished.");
            });
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingStartTest()
        {
            var stream = new ThrottledMarketDataStream();

            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                tasks.Add(new Task(() => stream.Start()));
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks _stream finished.");
                stream.End();
            });
        }


        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingEndTest()
        {
            var stream = new ThrottledMarketDataStream();

            var tasks = new List<Task>();
            stream.Start();

            for (int i = 0; i < 10; i++)
            {
                tasks.Add(new Task(() => stream.End()));
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks _stream.End() finished.");
            });
        }

        [TestMethod]
        public void ThrottledMarketDataStreamOnUpdateNullIgnore()
        {
            var stream = new ThrottledMarketDataStream();
            stream.OnUpdate(null);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingStartStopTest()
        {
            var stream = new ThrottledMarketDataStream();

            var tasks = new List<Task>();

            for (int i = 0; i < 10; i++)
            {
                if (i % 2 == 0)
                {
                    tasks.Add(new Task(() => stream.Start()));
                }
                else
                {
                    tasks.Add(new Task(() => stream.End()));
                }
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks _stream Start/End  finished.");
                stream.End();
            });
        }
    }
}
