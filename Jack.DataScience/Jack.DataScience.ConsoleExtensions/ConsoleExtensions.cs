using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;


namespace Jack.DataScience.ConsoleExtensions
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

        public static IEnumerable<string> GetParameters(this string[] args, string key, params string[] aliases)
        {
            List<string> keys = new List<string>() { key };
            keys.AddRange(aliases);

            int index = -1;
            do
            {
                index = args.ToList().FindIndex(index + 1, arg => keys.Any(k => k == arg));
                if (index >= 0 && index < args.Length - 1) yield return args[index + 1];
            }
            while (index > -1);
        }

        public static bool HasParameter(this string[] args, string key, params string[] aliases)
        {
            List<string> keys = new List<string>() { key };
            keys.AddRange(aliases);
            return args.Any(arg => keys.Any(k => k == arg));
        }

        public static int GetIntegerParameter(this string[] args, string key, int defaultValue, params string[] aliases)
        {
            string value = args.GetParameter(key, aliases);
            int result = defaultValue;
            int.TryParse(value, out result);
            return result;
        }

        public static int? GetNullableIntegerParameter(this string[] args, string key, params string[] aliases)
        {
            string value = args.GetParameter(key, aliases);
            int result;
            if (int.TryParse(value, out result))
                return result;
            else
                return null;
        }

        public static long GetLongParameter(this string[] args, string key, long defaultValue, params string[] aliases)
        {
            string value = args.GetParameter(key, aliases);
            long result = defaultValue;
            long.TryParse(value, out result);
            return result;
        }

        public static long? GetNullableLongParameter(this string[] args, string key, params string[] aliases)
        {
            string value = args.GetParameter(key, aliases);
            long result;
            if (long.TryParse(value, out result))
                return result;
            else
                return null;
        }

        public static DateTime GetDateTimeParameter(this string[] args, string key, DateTime defaultValue, string format, DateTimeStyles styles, params string[] aliases)
        {
            string value = args.GetParameter(key, aliases);
            DateTime result = defaultValue;
            DateTime.TryParseExact(value, format, null, styles, out result);
            return result;
        }

        public static DateTime? GetNullableDateTimeParameter(this string[] args, string key, string format, DateTimeStyles styles, params string[] aliases)
        {
            string value = args.GetParameter(key, aliases);
            DateTime result;
            if (DateTime.TryParseExact(value, format, null, styles, out result))
                return result;
            else
                return null;
        }
    }
}
