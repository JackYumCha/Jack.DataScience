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
            for(int i = 1; i < 6; i++)
            {

                AthenaParserLogger athenaParserLogger = new AthenaParserLogger();
                Debug.WriteLine($"****** Begin File {i} ******");
                var filename = $"{AppContext.BaseDirectory}/query{i}.sql";
                Debug.WriteLine($"File {i}: {filename}");
                var query = File.ReadAllText(filename);
                try
                {
                    var pipes = query.ParseAthenaPipes(athenaParserLogger);
                    Debug.WriteLine($"****** End File {i} ******");
                    Debug.WriteLine($"****** Json File {i} ******");
                    var tree = JsonConvert.SerializeObject(pipes, Formatting.Indented);
                    Debug.WriteLine($"****** Parsed File {i} ******");
                    Debug.WriteLine(pipes.ToQueryString().StripEmptyLines());
                }
                catch(Exception ex)
                {
                    Debug.WriteLine(athenaParserLogger.ToString());
                    Debug.Write(ex.Message);
                }
            }
        }
    }
}
