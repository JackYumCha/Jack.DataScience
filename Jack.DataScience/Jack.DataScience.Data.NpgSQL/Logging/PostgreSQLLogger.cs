using System;
using Npgsql.Logging;
using Serilog;

namespace Jack.DataScience.Data.NpqSQL.Logging
{
    public class PostgreSQLLogger: NpgsqlLogger
    {
        private readonly ILogger logger;
        public PostgreSQLLogger(ILogger logger)
        {
            this.logger = logger;
        }
        public NpgsqlLogLevel LogLevel { get; set; }

        public override bool IsEnabled(NpgsqlLogLevel level)
        {
            return level >= LogLevel;
        }

        public override void Log(NpgsqlLogLevel level, int connectorId, string msg, Exception exception = null)
        {
            if (IsEnabled(level))
            {
                if(exception != null)
                {
                    logger.Error(exception, $"Connector: {connectorId}; {msg} - {{@error}}");
                }
                else
                {
                    switch (level)
                    {
                        case NpgsqlLogLevel.Trace:
                            logger.Verbose($"Connector: {connectorId}; {msg}");
                            break;
                        case NpgsqlLogLevel.Debug:
                            logger.Debug($"Connector: {connectorId}; {msg}");
                            break;
                        case NpgsqlLogLevel.Info:
                            logger.Information($"Connector: {connectorId}; {msg}");
                            break;
                        case NpgsqlLogLevel.Warn:
                            logger.Warning($"Connector: {connectorId}; {msg}");
                            break;
                        case NpgsqlLogLevel.Error:
                            logger.Error(exception, $"Connector: {connectorId}; {msg} - {{@error}}");
                            break;
                        case NpgsqlLogLevel.Fatal:
                            logger.Fatal(exception, $"Connector: {connectorId}; {msg} - {{@error}}");
                            break;
                    }
                }
            }
        }
    }
}
