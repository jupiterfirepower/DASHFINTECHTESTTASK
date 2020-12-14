using System;
using System.Threading.Tasks.Dataflow;

namespace MarketDataAggregator
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using MarketDataAggregator.Helpers;
    using MarketDataAggregator.DomainModel;
    using MarketDataThrottling.Contracts;

    internal class DataAggregator
    {
        private BufferBlock<MarketDataUpdate> _source = null;  

        private BatchBlock<MarketDataUpdate> _batchBlock = null;

        private TransformBlock<MarketDataUpdate[], List<MarketDataUpdate>> _transformBlock = null;

        private ActionBlock<List<MarketDataUpdate>> _writer = null;

        private bool _start = false;

        private Timer _timer = null;

        private IList<IAggreagatedMarketDataObserver> _watchers;

        public DataAggregator(IList<IAggreagatedMarketDataObserver> watchers)
        {
            _watchers = watchers;
        }

        private void CreateMarketDataProcessingDataFlow()
        {
            _source = new BufferBlock<MarketDataUpdate>(new DataflowBlockOptions() { BoundedCapacity = DataAggregatorAppSettings.BufferBoundedCapacity });
            _batchBlock = new BatchBlock<MarketDataUpdate>(batchSize: DataAggregatorAppSettings.BatchSize, new GroupingDataflowBlockOptions() { Greedy = true });

            _writer = new ActionBlock<List<MarketDataUpdate>>(cd =>
            {
                Console.WriteLine("-------------------------------------------------------");
                cd.ForEach(d => Console.WriteLine(d));
                Console.WriteLine("*******************************************************");
                // snapshot work with current copy data - thread safe.
                _watchers?.ToList().ForEach(watcher => watcher.OnUpdate(cd.ToArray()));
            });

            _transformBlock = new TransformBlock<MarketDataUpdate[], List<MarketDataUpdate>>(e =>
            {
                var helper = new MergeDataHelper();
                return helper.MergeData(e);
            });

            _source.LinkTo(_batchBlock, new DataflowLinkOptions() { PropagateCompletion = true });

            _batchBlock.LinkTo(_transformBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            _transformBlock.LinkTo(_writer, new DataflowLinkOptions() { PropagateCompletion = true });

            _timer = new Timer(x => {
                _batchBlock.TriggerBatch();
            });
            _timer.Change(DataAggregatorAppSettings.TimerDueTimeMiliseconds, DataAggregatorAppSettings.TimerPeriodMiliseconds);
        }

        public void Start()
        {
            if(!_start && _timer == null)
            {
                CreateMarketDataProcessingDataFlow();
                _start = true;
            }
        }

        private void ReleaseForGCDataFlow()
        {
            _source = null;
            _batchBlock = null;
            _transformBlock = null;
            _writer = null;
            _timer = null;
        }

        private void BlockComplete()
        {
            _source.Complete();
            _batchBlock.Complete();
            _transformBlock.Complete();
            _writer.Complete();
        }

        public async Task CompleteAsync()
        {
            try
            {
                if (_start && _timer != null)
                {
                    await _timer.DisposeAsync();

                    BlockComplete();
                    await Task.WhenAll(_writer.Completion).ConfigureAwait(false);
                }
                Console.WriteLine("DataAggregator stopped.");
            }
            catch (AggregateException ex)
            {
                ex.InnerExceptions?.ToList().ForEach(e => Console.WriteLine(e.Message));
            }
            finally
            {
                ReleaseForGCDataFlow();
                _start = false;
            }
        }

        ~DataAggregator()
        {
            try
            {
                 AsyncHelper.RunSync(() => CompleteAsync());
            }
            catch 
            {
            }
        }

        public async Task SendDataAsync(MarketDataUpdate curItemData)
        {
            if (_start && curItemData != null)
                await _source.SendAsync(curItemData);
            else
                await Task.CompletedTask;
        }
    }
}
