using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Jack.DataScience.ProcessControl
{
    public static class TemplateExtensions
    {
        private static Regex replaceSystemKey = new Regex(@"<#@(\w+)\(([\w-\.,;#@%\$]*)\)@#>");

        public static string PopulateKeys(this string value)
        {
            value = replaceSystemKey
                .Replace(value, (match) =>
                {
                    var method = match.Groups[1].Value;
                    switch (method.ToLower())
                    {
                        case "date":
                            return DateTime.UtcNow.ToString(match.Groups[2].Value);
                        case "env":
                            return Environment.GetEnvironmentVariable(match.Groups[2].Value);
                    }
                    return "";
                });
            return value;
        }
    }
}
