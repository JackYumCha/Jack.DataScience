using System;
using System.Collections.Generic;

namespace MvcAngular.Generator
{
    internal static class LinqExtensions
    {
        public static int IndexOf<T>(this IEnumerable<T> enumerable, Func<T, bool> predicate)
        {
            int index = -1;
            foreach (var item in enumerable)
            {
                index += 1;
                if (predicate.Invoke(item)) return index;
            }
            return -1;
        }

        public static int LastIndexOf<T>(this IEnumerable<T> enumerable, Func<T, bool> predicate)
        {
            int index = -1;
            int found = -1;
            foreach (var item in enumerable)
            {
                index += 1;
                if (predicate.Invoke(item))
                    found = index;
            }
            return found;
        }
    }
}
