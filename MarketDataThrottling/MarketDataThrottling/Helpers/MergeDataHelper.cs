using MoreLinq;
using System;
using System.Collections.Generic;
using System.Linq;
using MarketDataAggregator.DomainModel;

namespace MarketDataAggregator.Helpers
{
    public class MergeDataHelper
    {
        private List<MarketDataUpdate>  Algo1MergeData(MarketDataUpdate[] inputSource)
        {
            return inputSource.GroupBy(
                                x => new { x.InstrumentId },
                                (x, y) => new MarketDataUpdate
                                {
                                    InstrumentId = x.InstrumentId,
                                    Fields = y
                                            .SelectMany(z => z.Fields)
                                            .ToLookup(z => z.Key, z => z.Value)
                                            .ToDictionary(z => z.Key, z => z.Last())
                                }).OrderBy(c => c.InstrumentId)
                                .ToList();
        }

        private List<MarketDataUpdate> Algo2MergeData(MarketDataUpdate[] inputSource)
        {
            var grouped = inputSource.ToLookup(p => p.InstrumentId);

            var result = new List<MarketDataUpdate>();

            foreach (var dataGroup in grouped)
            {
                var currentResult = new MarketDataUpdate
                {
                    InstrumentId = dataGroup.Key,
                    Fields = new Dictionary<byte, long>()
                };

                var current = dataGroup.SelectMany(c => c.Fields).ToList();
                current.ForEach(x => currentResult.Fields[x.Key] = x.Value);
                result.Add(currentResult);
            }

            return result.OrderBy(c => c.InstrumentId).ToList();
        }

        private List<MarketDataUpdate> Algo3MergeData(MarketDataUpdate[] inputSource)
        {
            var result = inputSource
                .Reverse()
                .GroupBy(c => c.InstrumentId)
                .Select(g => {

                var currentResult = new MarketDataUpdate
                {
                    InstrumentId = g.Key,
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

        public List<MarketDataUpdate> MergeData(MarketDataUpdate[] inputSource)
        {
            return inputSource != null ? Algo1MergeData(inputSource) : new List<MarketDataUpdate>();
        }

        public List<MarketDataUpdate> Merge2DataTest(MarketDataUpdate[] inputSource)
        {
            return inputSource != null ? Algo2MergeData(inputSource) : new List<MarketDataUpdate>();
        }

        public List<MarketDataUpdate> Merge3DataTest(MarketDataUpdate[] inputSource)
        {
            return inputSource != null ? Algo3MergeData(inputSource) : new List<MarketDataUpdate>();
        }

    }
}
