using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Jack.DataScience.Algorithms.UnionFind
{
    public static class DisjointSetExtensions
    {
        /// <summary>
        /// remove the duplicates and return ordered array
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static string[] MapUnionFindKeys(this IEnumerable<string> keys) => keys.Distinct().OrderBy(key => key).ToArray();
        /// <summary>
        /// remove the duplicates and return ordered array
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static int[] MapUnionFindKeys(this IEnumerable<int> keys) => keys.Distinct().OrderBy(key => key).ToArray();
        /// <summary>
        /// remove the duplicates and return ordered array
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static long[] MapUnionFindKeys(this IEnumerable<long> keys) => keys.Distinct().OrderBy(key => key).ToArray();
        /// <summary>
        /// build dictionary to allow mapping key to index with O(1) time
        /// </summary>
        /// <param name="sets"></param>
        /// <returns></returns>
        public static Dictionary<string, int> BuildDictionary(this string[] sets)
        {
            Dictionary<string, int> dict = new Dictionary<string, int>();
            for (int i = 0; i < sets.Length; i++) dict.Add(sets[i], i);
            return dict;
        }
        /// <summary>
        /// build dictionary to allow mapping key to index with O(1) time
        /// </summary>
        /// <param name="sets"></param>
        /// <returns></returns>
        public static Dictionary<long, int> BuildDictionary(this long[] sets)
        {
            Dictionary<long, int> dict = new Dictionary<long, int>();
            for (int i = 0; i < sets.Length; i++) dict.Add(sets[i], i);
            return dict;
        }
        /// <summary>
        /// build dictionary to allow mapping key to index with O(1) time
        /// </summary>
        /// <param name="sets"></param>
        /// <returns></returns>
        public static Dictionary<int, int> BuildDictionary(this int[] sets)
        {
            Dictionary<int, int> dict = new Dictionary<int, int>();
            for (int i = 0; i < sets.Length; i++) dict.Add(sets[i], i);
            return dict;
        }
        /// <summary>
        /// map key to index with binary serach
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sets"></param>
        /// <returns></returns>
        public static int UnionFindIndex(this string key, string[] sets) => Array.BinarySearch(sets, key);
        /// <summary>
        /// map key to index with binary serach
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sets"></param>
        /// <returns></returns>
        public static int UnionFindIndex(this long key, long[] sets) => Array.BinarySearch(sets, key);
        /// <summary>
        /// map key to index with binary serach
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sets"></param>
        /// <returns></returns>
        public static int UnionFindIndex(this int key, int[] sets) => Array.BinarySearch(sets, key);

        /// <summary>
        /// get each subgraph as an int[]
        /// </summary>
        /// <param name="disjointSet"></param>
        /// <returns></returns>
        public static IEnumerable<int[]> Subgraphs(this DisjointSet disjointSet)
        {
            Dictionary<int, List<int>> dict = new Dictionary<int, List<int>>();
            for(int i = 0; i < disjointSet.size; i++)
            {
                int p = disjointSet.Find(i);
                if (dict.ContainsKey(p))
                {
                    dict[p].Add(i);
                }
                else
                {
                    List<int> list = new List<int>() { i };
                    dict.Add(p, list);
                }
            }
            return dict.Values.Select(list => list.OrderBy(i => i).ToArray());
        }
    }
}
