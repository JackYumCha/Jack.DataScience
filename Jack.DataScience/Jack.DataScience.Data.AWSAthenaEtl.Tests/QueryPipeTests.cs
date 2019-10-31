using Jack.DataScience.Data.AWSAthenaEtl;
using System;
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
                var filename = $"{AppContext.BaseDirectory}/query{i}.sql";
                var query = File.ReadAllText(filename);
                var pipes = query.ParseAthenaPipes();
            }
        }
    }
}
