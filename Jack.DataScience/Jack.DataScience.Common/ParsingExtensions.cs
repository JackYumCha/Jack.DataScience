using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;

namespace Jack.DataScience.Common
{
    public static class ParsingExtensions
    {
        public static string RemoveCrLfAtEnd(this string value)
        {
            return Regex.Replace(value, "[ \n\r]+", "");
        }

        public static string RemovePatterns(this string value, params string[] patterns)
        {
            string result = value;
            foreach(string pattern in patterns)
            {
                result = Regex.Replace(value, pattern, "");
            }
            return result;
        }

        public static int? TryToInt(this string value)
        {
            int result;
            return int.TryParse(
                value, 
                NumberStyles.AllowThousands & NumberStyles.AllowLeadingWhite & NumberStyles.AllowTrailingWhite, 
                CultureInfo.InvariantCulture,
                out result) ? new int?(result) : null;
        }
    }
}
