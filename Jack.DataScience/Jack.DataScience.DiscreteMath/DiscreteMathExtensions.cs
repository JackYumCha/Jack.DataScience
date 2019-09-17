using System;
using System.Collections.Generic;
using System.Linq;

namespace Jack.DataScience.DiscreteMath
{
    public static class DiscreteMathExtensions
    {
        public static IEnumerable<int> Range(this int count)
        {
            for (int i = 0; i < count; i++) yield return i;
        }
        public static IEnumerable<int> Range(this int count, int from)
        {
            count += from;
            for (int i = from; i < count; i++) yield return i;
        }

        public static IEnumerable<T[]> Combine<T>(this T[] items, int take)
        {
            var arr = items.ToArray();
            foreach (var indices in arr.Length.IterativeCombine(take)) yield return indices.Select(i => arr[i]).ToArray();
        }

        public static IEnumerable<int[]> Combine(this int count, int take)
        {
            var indices = take.Range().ToArray();
            yield return indices.ToArray();
            while (indices.ShiftCombinationIndex(count)) yield return indices.ToArray();
        }

        /// <summary>
        /// Perform combination without creating new int[]
        /// </summary>
        /// <param name="count">total number</param>
        /// <param name="take">number to take</param>
        /// <returns></returns>
        public static IEnumerable<int[]> IterativeCombine(this int count, int take)
        {
            var indices = take.Range().ToArray();
            yield return indices;
            while (indices.ShiftCombinationIndex(count)) yield return indices;
        }

        public static bool ShiftCombinationIndex(this int[] indices, int max)
        {
            int pos = indices.Length;
            bool shifting = true;
            while (shifting && (pos--) > 0)
            {
                indices[pos]++;
                if (indices[pos] <= max + pos - indices.Length)
                {
                    shifting = false;
                    while (pos++ < indices.Length - 1) indices[pos] = indices[pos - 1] + 1;
                }
            }
            return !shifting;
        }

        public static IEnumerable<T[]> Permute<T>(this T[] items)
        {
            foreach (var permutation in items.PermuteSwaps(items.Length)) yield return permutation.ToArray();
        }

        public static IEnumerable<T[]> Permute<T>(this T[] items, int take)
        {
            foreach (var combination in items.Combine(take))
                foreach (var permutation in combination.PermuteSwaps(combination.Length))
                    yield return permutation.ToArray();
        }

        public static IEnumerable<T[]> IterativePermute<T>(this T[] items)
        {
            foreach (var permutation in items.PermuteSwaps(items.Length)) yield return permutation;
        }

        private static IEnumerable<T[]> PermuteSwaps<T>(this T[] items, int take)
        {
            if (take == 1)
                yield return items;
            else
            {
                int limit = take - 1;
                bool even = (take & 1) == 0;
                for (int i = 0; i < limit; i++)
                {
                    foreach (var swap in PermuteSwaps(items, limit)) yield return swap;
                    if (even) items.Swap(i, limit);
                    else items.Swap(0, limit);
                }
                foreach (var swap in PermuteSwaps(items, limit)) yield return swap;
            }
            yield break;
        }

        private static void Swap<T>(this T[] array, int a, int b)
        {
            T tmp = array[a];
            array[a] = array[b];
            array[b] = tmp;
        }

        public static IEnumerable<T[]> SlidingBox<T>(this T[] items, int size)
        {
            T[] result = new T[size];
            for (int i = 0; i <= items.Length - size; i++)
            {
                Array.Copy(items, i, result, 0, size);
                yield return result;
            }
        }

        public static IEnumerable<IEnumerable<T>> SkipEach<T>(this IEnumerable<T> items)
        {
            var arr = items.ToArray();
            int count = arr.Length;
            for (int i = 0; i < count; i++) yield return arr.SkipAt(i);
        }

        public static IEnumerable<KeyValuePair<T, IEnumerable<T>>> PickEach<T>(this IEnumerable<T> items)
        {
            var arr = items.ToArray();
            int count = arr.Length;
            for (int i = 0; i < count; i++) yield return new KeyValuePair<T, IEnumerable<T>>(arr[i], arr.SkipAt(i));
        }

        public static IEnumerable<T> SkipAt<T>(this IEnumerable<T> items, int index)
        {
            int i = 0;
            foreach (var item in items)
                if (i++ != index) yield return item;
        }


        public static long Permutation(this long N)
        {
            long result = 1;
            while (N > 0) result *= N--;
            return result;
        }
        public static long Permutation(this int N)
        {
            long result = 1;
            while (N > 0) result *= N--;
            return result;
        }

        public static long Permutation(this long N, long M)
        {
            long result = 1;
            while (M-- > 0) result *= N--;
            return result;
        }

        public static long Permutation(this int N, int M)
        {
            long result = 1;
            while (M-- > 0) result *= N--;
            return result;
        }

        public static long Combination(this int N, int M)
        {
            if (M + M > N) M = N - M;
            long result = 1L;
            for (long i = 0L; i < M; i++)
            {
                result *= N - i;
                result /= 1 + i;
            }
            return result;
        }
        public static long Combination(this long N, long M)
        {
            if (M + M > N) M = N - M;
            long result = 1L;
            for (long i = 0L; i < M; i++)
            {
                result *= N - i;
                result /= 1 + i;
            }
            return result;
        }

    }
}
