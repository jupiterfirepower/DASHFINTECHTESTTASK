using Microsoft.VisualStudio.TestTools.UnitTesting;
using MarketDataAggregator;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MarketDataAggregator.DomainModel;
using System;

namespace UnitTestMarketDataThrottling
{
    [TestClass]
    public class UnitTestMultithreadingMarketDataThrottling
    {
        private static ThrottledMarketDataStream _stream = new ThrottledMarketDataStream();

        private void ThrottledMarketDataStreamMultithreadingAddWatchers(List<Client> list)
        {
            // create a bunch of threads
            List<Thread> threads = new List<Thread>();
            list.ForEach(x => threads.Add(new Thread(() => ThreadMethodAddWatcher(x))));

            // start them
            threads.ForEach(x => x.Start());

            // wait for them to finish
            threads.ForEach(x => x.Join());
        }

        [TestMethod]
        public void ThrottledMarketDataStreamMultithreadingAddWatcherTest()
        {
            List<Client> list = new List<Client>();
            list.AddRange(new Client[] { new Client(), new Client(), new Client(), new Client(), new Client() });

            ThrottledMarketDataStreamMultithreadingAddWatchers(list);
            // this will not print untill all threads have completed
            System.Diagnostics.Debug.WriteLine("All threads finished.");
            Assert.IsTrue(_stream.WatcherCount == 5);
        }

        private static void ThreadMethodAddWatcher(Client c)
        {
            _stream.AddWatcher(c);
            System.Diagnostics.Debug.WriteLine("Thread ManagedThreadId : {0}", Thread.CurrentThread.ManagedThreadId);
        }

        private static void ThreadMethodRemoveWatcher(Client c)
        {
            _stream.RemoveWatcher(c);

            System.Diagnostics.Debug.WriteLine("Thread ManagedThreadId : {0}", Thread.CurrentThread.ManagedThreadId);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamMultithreadingRemoveWatcherTest()
        {
            List<Client> list = new List<Client>();
            list.AddRange(new Client[] { new Client(), new Client(), new Client(), new Client(), new Client(), new Client(), new Client(), new Client(), null });

            ThrottledMarketDataStreamMultithreadingAddWatchers(list);
            Assert.IsTrue(_stream.WatcherCount == 9);

            // create a bunch of threads
            List<Thread> threads = new List<Thread>();
            list.ForEach(x => threads.Add(new Thread(() => ThreadMethodRemoveWatcher(x))));

            // start them
            threads.ForEach(x => x.Start());

            // wait for them to finish
            threads.ForEach(x => x.Join());

            // this will not print untill all threads have completed
            System.Diagnostics.Debug.WriteLine("All threads finished.");
            Thread.Sleep(100);
            System.Diagnostics.Debug.WriteLine($"WatcherCount - {_stream.WatcherCount}");
            Assert.IsTrue(_stream.WatcherCount == 0);
        }

        private static async Task ThreadMethodAddWatcherAync(Client c)
        {
            await Task.CompletedTask;
            _stream.AddWatcher(c);
            System.Diagnostics.Debug.WriteLine("ThreadMethodAddWatcherAync Thread ManagedThreadId : {0}", Thread.CurrentThread.ManagedThreadId);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingAddWatcherAyncTest()
        {
            List<Client> list = new List<Client>();
            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                var currentClient = new Client();
                list.Add(currentClient);
                tasks.Add(new Task(async () => await ThreadMethodAddWatcherAync(currentClient)));
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
            Thread.Sleep(1000);
            Assert.IsTrue(_stream.WatcherCount == 100);
        }

        private static async Task ThreadMethodRemoveWatcherAync(Client c)
        {
            await Task.CompletedTask;
            _stream.RemoveWatcher(c);
            System.Diagnostics.Debug.WriteLine("ThreadMethodRemoveWatcherAync Thread ManagedThreadId : {0}", Thread.CurrentThread.ManagedThreadId);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingAddRemoveWatcherAyncTest()
        {
            List<Client> list = new List<Client>();
            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                var currentClient = new Client();
                list.Add(currentClient);
                tasks.Add(new Task(async () => await ThreadMethodAddWatcherAync(currentClient)));
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
            Assert.IsTrue(_stream.WatcherCount == 100);

            tasks.Clear();

            list.ForEach(c => tasks.Add(new Task(async () => await ThreadMethodRemoveWatcherAync(c))));

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks remove watcher finished.");
            });
            Thread.Sleep(1000);
            Assert.IsTrue(_stream.WatcherCount == 0);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingCombinedAddRemoveWatcherTest()
        {
            List<Client> list = new List<Client>();
            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                var currentClient = new Client();
                list.Add(currentClient);
                tasks.Add(new Task(async () => await ThreadMethodAddWatcherAync(currentClient)));
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
            Assert.IsTrue(_stream.WatcherCount == 100);

            tasks.Clear();

            for (int i = 0; i < list.Count - 1; i++)
            {
                if( i%2 ==0 )
                {
                    tasks.Add(new Task(async () => await ThreadMethodAddWatcherAync(new Client())));
                }
                else
                {
                    tasks.Add(new Task(async () => await ThreadMethodRemoveWatcherAync(list[i])));
                }
            }

            list.ForEach(c => tasks.Add(new Task(async () => await ThreadMethodRemoveWatcherAync(c))));

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
            Assert.IsTrue(_stream.WatcherCount == 50);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingOnUpdateTest()
        {
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

                    _stream.OnUpdate(update);
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
                tasks.Add(new Task(() =>_stream.OnUpdate(list[i]) ));
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
            var tasks = new List<Task>();

            for (int i = 0; i < 100; i++)
            {
                tasks.Add(new Task(() => _stream.Start()));
            }

            Parallel.ForEach(tasks, task =>
            {
                task.Start();
            });

            Task.WhenAll(tasks).ContinueWith(done =>
            {
                // this will not print untill all tasks have completed.
                System.Diagnostics.Debug.WriteLine("All tasks _stream finished.");
                _stream.End();
            });
        }


        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingEndTest()
        {
            var tasks = new List<Task>();
            _stream.Start();

            for (int i = 0; i < 10; i++)
            {
                tasks.Add(new Task(() => _stream.End()));
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
            _stream.OnUpdate(null);
        }

        [TestMethod]
        public void ThrottledMarketDataStreamTasksMultithreadingStartStopTest()
        {
            var tasks = new List<Task>();

            for (int i = 0; i < 10; i++)
            {
                if (i % 2 == 0)
                {
                    tasks.Add(new Task(() => _stream.Start()));
                }
                else
                {
                    tasks.Add(new Task(() => _stream.End()));
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
                _stream.End();
            });
        }
    }
}
