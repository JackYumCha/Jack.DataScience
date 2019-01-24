using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class StringRegexExtensions
    {
        public static string MakeRegexExtraction(this string value, Regex datePattern)
        {
            var match = datePattern.Match(value);
            var dateKey = "";
            for (int i = 1; i < match.Groups.Count; i++)
            {
                dateKey += match.Groups[i].Value;
            }
            return dateKey;
        }
    }
}
