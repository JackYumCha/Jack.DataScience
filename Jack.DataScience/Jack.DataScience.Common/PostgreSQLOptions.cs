using System;
using System.Collections.Generic;
using System.Text;
using Npgsql.Logging;

namespace Jack.DataScience.Common
{
    public class PostgreSQLOptions
    {
        public string ConnectionString { get; set; }
        public NpgsqlLogLevel LogLevel { get; set; }
    }
}
