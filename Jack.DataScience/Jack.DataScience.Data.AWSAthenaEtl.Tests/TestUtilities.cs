using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Jack.DataScience.Data.AWSAthenaEtl.Tests
{
    public static class TestUtilities
    {
        public static void Print(this LinkedList<int> states)
        {
            Debug.WriteLine($"Curent State is: {string.Join("->", states.Select(i => i.ToString()))}");
        }

        public static void Print(this StateMachineQueryContext context)
        {
            context.state.Print();
            Debug.WriteLine($"SQL Query: {context.query}");
            if(!context.state.Any()) Debug.WriteLine($"*** End of Pipes ***");
        }

        public static void RunWithResult(this StateMachineQueryContext context, StateMachineQueryResult result)
        {
            context.result = result;
            context.ExecuteStateMachineQueryContext();
            if(context.settings.Variables != null && context.settings.Variables.Any())
            {
                Debug.WriteLine("Context Variables:");
                foreach(var kv in context.settings.Variables)
                {
                    Debug.WriteLine($"    Variable: {kv.Key} -> {kv.Value}");
                }
            }
            context.Print();
        }

        public static StateMachineQueryContext BuildFromFile(this DateTime date, int fileIndex)
        {
            Debug.WriteLine($"****** Begin File {fileIndex} ******");
            var filename = $"{AppContext.BaseDirectory}/query{fileIndex}.sql";
            Debug.WriteLine($"File {fileIndex}: {filename}");
            var query = File.ReadAllText(filename);
            EtlSettings etlSettings = new EtlSettings()
            {
                SourceType = EtlSourceEnum.AmazonAthenaPipes,
                AthenaQueryPipesSource = new AthenaQueryPipesSetting()
                {
                    AthenaSQL = query,
                    DaysAgo = -2,
                    DateFormat = "yyyyMMdd",
                    TempDatabase = "ctascache",
                    TempDataPath = "",
                    DefaultOutputLocation = "s3://athena-ctas-cache-855250023996/testpath/",
                    Caches = File.ReadAllText($"{AppContext.BaseDirectory}/caches.json"),
                }
            };
            return etlSettings.BuildStateMachineQueryContext(date);
        }
    }
}
