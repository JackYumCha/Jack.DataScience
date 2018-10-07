using System;
using System.Collections.Generic;
using System.Linq;

namespace Jack.DataScience.Common
{
 
    public static class ParameterExtensions
    {
        public static string ParseConsoleParameter(this string[] args, string command, params string[] alias)
        {
            if (alias == null)
                alias = new string[] { };
            int index = args.LastIndexOf(arg => arg.ToLower() == command.ToLower() || alias.Any(aliasName => arg.ToLower() == aliasName.ToLower()));
            return index > -1 && index < args.Length - 1 ? args[index + 1].Replace("\"", "") : null;
        }

        public static bool AssertConsoleParameter(this string[] args, string command, params string[] alias)
        {
            if (alias == null)
                alias = new string[] { };
            return args.Any(arg => arg.ToLower() == command.ToLower() || alias.Any(aliasName => arg.ToLower() == aliasName.ToLower()));
        }

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
