using MarketDataAggregator.DomainModel;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("UnitTestMarketDataThrottling")]
namespace MarketDataThrottling.Contracts
{
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
}
