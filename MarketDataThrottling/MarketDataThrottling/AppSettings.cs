using Newtonsoft.Json;

namespace MarketDataThrottling
{
    [JsonObject("DataAggregatorSettings")]
    public class AppSettings
    {
        [JsonProperty("TimerDueTimeMiliseconds")]
        public int TimerDueTimeMiliseconds { get; set; }

        [JsonProperty("TimerPeriodMiliseconds")]
        public int TimerPeriodMiliseconds { get; set; }

        [JsonProperty("BufferBoundedCapacity")]
        public int BufferBoundedCapacity { get; set; }

        [JsonProperty("BatchSize")]
        public int BatchSize { get; set; }
    }
}
