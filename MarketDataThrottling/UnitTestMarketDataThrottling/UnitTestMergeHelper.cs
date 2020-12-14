using Microsoft.VisualStudio.TestTools.UnitTesting;
using MarketDataAggregator.Helpers;
using MarketDataAggregator.DomainModel;
using System.Collections.Generic;
using System;
using System.Linq;
using MarketDataAggregator;

namespace UnitTestMarketDataThrottling
{
    [TestClass]
    public class UnitTestMergeHelper
    {
        private readonly List<MarketDataUpdate> _testMarketDataDource = null;

        public UnitTestMergeHelper()
        {
            _testMarketDataDource = new List<MarketDataUpdate>{
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 1, 10 },
                                                                            { 4, 200 },
                                                                            { 12, 187 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_2",  Fields = new Dictionary<byte, long>() {
                                                                            { 1, 12 },
                                                                            { 4, 210 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 12, 189 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 2, 24 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_2",  Fields = new Dictionary<byte, long>() {
                                                                            { 5, 120 }
                                                                          }
                                      }
            };
        }

        public void CheckControlDataResult(List<MarketDataUpdate> result)
        {
            Assert.IsNotNull(result, "result cant be null");
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result.First().Fields.Count == 4);

            Assert.IsTrue(result.First().Fields.ContainsKey(1));
            Assert.IsTrue(result.First().Fields.ContainsKey(4));
            Assert.IsTrue(result.First().Fields.ContainsKey(12));
            Assert.IsTrue(result.First().Fields.ContainsKey(2));

            Assert.IsTrue(result.First().Fields[1] == 10);
            Assert.IsTrue(result.First().Fields[4] == 200);
            Assert.IsTrue(result.First().Fields[12] == 189);
            Assert.IsTrue(result.First().Fields[2] == 24);

            Assert.IsTrue(result.Last().Fields.Count == 3);

            Assert.IsTrue(result.Last().Fields.ContainsKey(1));
            Assert.IsTrue(result.Last().Fields.ContainsKey(4));
            Assert.IsTrue(result.Last().Fields.ContainsKey(5));

            Assert.IsTrue(result.Last().Fields[1] == 12);
            Assert.IsTrue(result.Last().Fields[4] == 210);
            Assert.IsTrue(result.Last().Fields[5] == 120);
        }

        [TestMethod]
        public void ControlDataMergeMainTest()
        {
            var helper = new MergeDataHelper();
            var result = helper.MergeData(_testMarketDataDource.ToArray());

            CheckControlDataResult(result);
        }

        [TestMethod]
        public void ControlDataNullInputSourceMainTest()
        {
            var helper = new MergeDataHelper();
            var result = helper.MergeData(null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count == 0);
        }

        private MarketDataUpdate[] getSecondTestData()
        {
            var testMarketDataDource = new List<MarketDataUpdate>{
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 1, 10 },
                                                                            { 4, 200 },
                                                                            { 12, 187 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_2",  Fields = new Dictionary<byte, long>() {
                                                                            { 1, 12 },
                                                                            { 4, 210 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 12, 189 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 2, 24 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_2",  Fields = new Dictionary<byte, long>() {
                                                                            { 5, 120 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 12, 289 }
                                                                          }
                                      }
            };
            return testMarketDataDource.ToArray();
        }

        public void CheckSecondControlDataResult(List<MarketDataUpdate> result)
        {
            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result.First().Fields.Count == 4);
            Assert.IsTrue(result.First().Fields[12] == 289);
        }

        [TestMethod]
        public void ControlDataMergeMainSecondTest()
        {
            var testMarketDataDource = getSecondTestData();

            var helper = new MergeDataHelper();
            var result = helper.MergeData(testMarketDataDource.ToArray());
            CheckSecondControlDataResult(result);
        }


        [TestMethod]
        public void ControlDataMergeAlgo2Test()
        {
            var helper = new MergeDataHelper();
            var result = helper.Merge2DataTest(_testMarketDataDource.ToArray());

            CheckControlDataResult(result);
        }

        [TestMethod]
        public void ControlDataNullInputSourceMergeAlgo2Test()
        {
            var helper = new MergeDataHelper();
            var result = helper.Merge2DataTest(null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count == 0);
        }

        [TestMethod]
        public void ControlDataMergeAlgo2SecondTest()
        {
            var testMarketDataDource = getSecondTestData();

            var helper = new MergeDataHelper();
            var result = helper.Merge2DataTest(testMarketDataDource.ToArray());
            CheckSecondControlDataResult(result);
        }

        [TestMethod]
        public void ControlDataMergeAlgo3Test()
        {
            var helper = new MergeDataHelper();
            var result = helper.Merge3DataTest(_testMarketDataDource.ToArray());

            CheckControlDataResult(result);
        }

        [TestMethod]
        public void ControlDataNullInputSourceMergeAlgo3TTest()
        {
            var helper = new MergeDataHelper();
            var result = helper.Merge3DataTest(null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count == 0);
        }

        [TestMethod]
        public void ControlDataMergeAlgo3SecondTest()
        {
            var testMarketDataDource = getSecondTestData();

            var helper = new MergeDataHelper();
            var result = helper.Merge3DataTest(testMarketDataDource.ToArray());
            CheckSecondControlDataResult(result);
        }


        private MarketDataUpdate[] getThirdTestData()
        {
            var testMarketDataDource = new List<MarketDataUpdate>{
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 1, 10 },
                                                                            { 4, 200 },
                                                                            { 12, 187 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 1, 12 },
                                                                            { 4, 210 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 12, 189 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_1",  Fields = new Dictionary<byte, long>() {
                                                                            { 2, 24 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_2",  Fields = new Dictionary<byte, long>() {
                                                                            { 5, 120 }
                                                                          }
                                      },
                 new MarketDataUpdate() { InstrumentId = "AAPL_2",  Fields = new Dictionary<byte, long>() {
                                                                            { 12, 289 }
                                                                          }
                                      }
            };
            return testMarketDataDource.ToArray();
        }

        public void CheckControlThirdDataResult(List<MarketDataUpdate> result)
        {
            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result.First().Fields.Count == 4);

            Assert.IsTrue(result.First().Fields.ContainsKey(1));
            Assert.IsTrue(result.First().Fields.ContainsKey(4));
            Assert.IsTrue(result.First().Fields.ContainsKey(12));
            Assert.IsTrue(result.First().Fields.ContainsKey(2));

            Assert.IsTrue(result.First().Fields[1] == 12);
            Assert.IsTrue(result.First().Fields[4] == 210);
            Assert.IsTrue(result.First().Fields[12] == 189);
            Assert.IsTrue(result.First().Fields[2] == 24);

            Assert.IsTrue(result.Last().Fields.Count == 2);

            Assert.IsTrue(result.Last().Fields.ContainsKey(5));
            Assert.IsTrue(result.Last().Fields.ContainsKey(12));
            
            Assert.IsTrue(result.Last().Fields[5] == 120);
            Assert.IsTrue(result.Last().Fields[12] == 289);
        }

        [TestMethod]
        public void ControlDataMergeMainThirdTest()
        {
            var testMarketDataDource = getThirdTestData();

            var helper = new MergeDataHelper();
            var result = helper.MergeData(testMarketDataDource.ToArray());

            CheckControlThirdDataResult(result);
        }

        [TestMethod]
        public void ControlDataMergeAlgo2ThirdTest()
        {
            var testMarketDataDource = getThirdTestData();

            var helper = new MergeDataHelper();
            var result = helper.Merge2DataTest(testMarketDataDource.ToArray());

            CheckControlThirdDataResult(result);
        }

        [TestMethod]
        public void ControlDataMergeAlgo3ThirdTest()
        {
            var testMarketDataDource = getThirdTestData();

            var helper = new MergeDataHelper();
            var result = helper.Merge3DataTest(testMarketDataDource.ToArray());

            CheckControlThirdDataResult(result);
        }

        public void CheckControlFourthDataResult(List<MarketDataUpdate> result)
        {
            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count == 0);
        }


        [TestMethod]
        public void ControlDataMergeMainFourthTest()
        {
            var emptyData = new List<MarketDataUpdate>();

            var helper = new MergeDataHelper();
            var result = helper.MergeData(emptyData.ToArray());

            CheckControlFourthDataResult(result);
        }

        [TestMethod]
        public void ControlDataMergeAlgo2FourthTest()
        {
            var emptyData = new List<MarketDataUpdate>();

            var helper = new MergeDataHelper();
            var result = helper.Merge2DataTest(emptyData.ToArray());

            CheckControlFourthDataResult(result);
        }

        [TestMethod]
        public void ControlDataMergeAlgo3FourthTest()
        {
            var emptyData = new List<MarketDataUpdate>();

            var helper = new MergeDataHelper();
            var result = helper.Merge3DataTest(emptyData.ToArray());

            CheckControlFourthDataResult(result);
        }

        [TestMethod]
        public void DataAggregatorAppSettingsTest()
        {
            DataAggregatorAppSettings.SetAppSettings(null);

            Assert.IsTrue(DataAggregatorAppSettings.TimerDueTimeMiliseconds == 0);
            Assert.IsTrue(DataAggregatorAppSettings.TimerPeriodMiliseconds == 200);
            Assert.IsTrue(DataAggregatorAppSettings.BatchSize == 2000);
            Assert.IsTrue(DataAggregatorAppSettings.BufferBoundedCapacity == 100);
        }
    }
}
