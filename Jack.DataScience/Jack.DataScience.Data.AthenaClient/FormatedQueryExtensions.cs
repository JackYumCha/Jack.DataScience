using Amazon.Athena.Model;
using Jack.DataScience.Data.AWSAthena;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Jack.DataScience.Data.AthenaClient
{
    public static class FormatedQueryExtensions
    {
        public static async Task<AthenaQueryFlatResult> GetQueryData(this AWSAthenaAPI athena, FormatedQuery query)
        {
            var sql = query.BuildQuerySQL();
            var request = await athena.ExecuteQuery(sql);
            return await athena.GetFlatResult(request);
        }

        private static Regex rgxFunction = new Regex(@"<#@([\-\w]+)\(([\w\W]*)\)@#>");
        private static Regex rgxParameter = new Regex(@"<#@([\-\w]+)@#>");

        public static string BuildQuerySQL(this FormatedQuery query)
        {
            string queryText = rgxFunction.Replace(query.Query, (Match m) =>
            {
                var functionName = m.Groups[1].Value;
                var parameter = m.Groups[2].Value;
                switch (functionName)
                {
                    case "date":
                        return DateTime.Now.ToString(parameter);
                    case "utc-date":
                        return DateTime.Now.ToString(parameter);
                }
                return "";
            });

            queryText = rgxParameter.Replace(queryText, (Match m) =>
            {
                var parameterName = m.Groups[1].Value;
                return query.Parameters.FirstOrDefault(p => p.Key == parameterName)?.Value ?? "";
            });

            return queryText;
        }
    }
}
