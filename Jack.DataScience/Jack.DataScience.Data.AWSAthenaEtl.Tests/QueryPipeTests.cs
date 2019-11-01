using Jack.DataScience.Data.AWSAthenaEtl;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.IO;
using Xunit;

namespace Jack.DataScience.Data.AWSAthenaEtl.Tests
{
    public class QueryPipeTests
    {
        [Fact(DisplayName = "Parse Queries")]
        public void ParseQuereis()
        {
            for(int i = 1; i < 4; i++)
            {
                Debug.WriteLine($"****** Begin File {i} ******");
                var filename = $"{AppContext.BaseDirectory}/query{i}.sql";
                Debug.WriteLine($"File {i}: {filename}");
                var query = File.ReadAllText(filename);
                var pipes = query.ParseAthenaPipes();
                var tree = JsonConvert.SerializeObject(pipes, Formatting.Indented);
                Debug.WriteLine(tree);
                Debug.WriteLine($"****** End File {i} ******");
            }
        }
    }
}
