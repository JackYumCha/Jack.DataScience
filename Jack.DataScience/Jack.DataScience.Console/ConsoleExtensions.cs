using System;
using System.Collections.Generic;
using System.Linq;

namespace Jack.DataScience.Console
{
    public static class ConsoleExtensions
    {
        public static string GetParameter(this string[] args, string key, params string[] aliases)
        {
            List<string> keys = new List<string>() { key };
            keys.AddRange(aliases);

            int index = args.ToList().FindIndex(arg => keys.Any(k => k == arg));

            if(index >= 0 && index < args.Length - 1)
            {
                return args[index + 1];
            }
            return null;
        }
    }
}
