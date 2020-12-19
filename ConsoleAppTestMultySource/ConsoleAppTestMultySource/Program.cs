using System;
using System.Threading.Tasks.Dataflow;


namespace ConsoleApp3
{
    class Program
    {
        private static BufferBlock<int> _buffer = new BufferBlock<int>(
                                        new DataflowBlockOptions { BoundedCapacity = 100 });

        

        static void Main(string[] args)
        {
            var da = new DataAggregator();
            da.TestPipeline();
            Console.ReadLine();
        }
    }
}
