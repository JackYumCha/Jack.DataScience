using System;
using System.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Jack.DataScience.Data.Converters
{
    public static class TypeDetectionExtensions
    {
        private static readonly Regex regexInteger = new Regex(@"^\d+$");
        private static readonly Regex regexDouble = new Regex(@"^\-?(\d+\.\d+|\d+)(e\-?\d+|)$", RegexOptions.IgnoreCase);
        private static readonly Regex regexBoolean = new Regex(@"^(true|false|1|0)$", RegexOptions.IgnoreCase);

        public static string DetectTypeString(this IEnumerable<string> items)
        {
            if (items.All(item => regexBoolean.IsMatch(item)))
                return "bool";
            if (items.All(item => regexInteger.IsMatch(item)))
                return "int";
            if (items.All(item => regexDouble.IsMatch(item)))
                return "double";
            return "string";
        }

        public static Type DetectType(this IEnumerable<string> items)
        {
            if (items.All(item => regexBoolean.IsMatch(item)))
                return typeof(bool);
            if (items.All(item => regexInteger.IsMatch(item)))
                return typeof(int);
            if (items.All(item => regexDouble.IsMatch(item)))
                return typeof(double);
            return typeof(string);
        }
    }
}
