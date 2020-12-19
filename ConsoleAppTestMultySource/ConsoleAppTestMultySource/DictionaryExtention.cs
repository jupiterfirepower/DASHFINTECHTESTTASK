namespace ConsoleApp3
{
    using System.Collections.Generic;
    using System.Linq;

    public class DictionaryExtention
    {
        public static Dictionary<K, V> Merge<K, V>(IEnumerable<Dictionary<K, V>> dictionaries)
        {
            return dictionaries.SelectMany(x => x)
                            .GroupBy(d => d.Key)
                            .ToDictionary(x => x.Key, y => y.First().Value);
        }
    }
}
