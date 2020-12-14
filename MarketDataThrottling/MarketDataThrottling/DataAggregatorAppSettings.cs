using MarketDataThrottling;

namespace MarketDataAggregator
{
    public static class DataAggregatorAppSettings
    {
        private static AppSettings _settings { get; set; }

        private const int DefaultTimerDueTimeMiliseconds = 0;
        private const int DefaultTimerPeriodMiliseconds = 200;
        private const int DefaultBufferBoundedCapacity = 100;
        private const int DefaultBatchSize = 2000;

        public static void SetAppSettings(AppSettings settings)
        {
            _settings = settings;
        }

        public static int TimerDueTimeMiliseconds => (_settings?.TimerDueTimeMiliseconds ?? -1) <= 0 ? DefaultTimerDueTimeMiliseconds : _settings.TimerDueTimeMiliseconds;

        public static int TimerPeriodMiliseconds => (_settings?.TimerPeriodMiliseconds ?? -1) <= 0 ? DefaultTimerPeriodMiliseconds : _settings.TimerPeriodMiliseconds;

        public static int BufferBoundedCapacity => (_settings?.BufferBoundedCapacity ?? -1) < 100 ? DefaultBufferBoundedCapacity : _settings.BufferBoundedCapacity;

        public static int BatchSize => (_settings?.BatchSize ?? -1) < 2000 ? DefaultBatchSize : _settings.BatchSize;

    }
}
