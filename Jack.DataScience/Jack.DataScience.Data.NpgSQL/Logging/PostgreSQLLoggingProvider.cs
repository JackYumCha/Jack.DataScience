using System;
using System.Collections.Generic;
using System.Text;
using Npgsql.Logging;
using Serilog;

namespace Jack.DataScience.Data.NpqSQL.Logging
{
    public class PostgreSQLLoggingProvider : INpgsqlLoggingProvider
    {
        private readonly ILogger logger;
        private readonly NpgsqlLogLevel logLevel;
        public PostgreSQLLoggingProvider(ILogger logger, NpgsqlLogLevel logLevel)
        {
            this.logger = logger;
            this.logLevel = logLevel;
        }
        NpgsqlLogger INpgsqlLoggingProvider.CreateLogger(string name)
        {
            return new PostgreSQLLogger(logger)
            {
                LogLevel = logLevel
            };
        }
    }
}
