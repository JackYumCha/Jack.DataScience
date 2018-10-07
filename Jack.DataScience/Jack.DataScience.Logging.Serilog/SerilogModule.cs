using System;
using System.Collections.Generic;
using System.Text;
using Autofac;
using Serilog;
using Serilog.Events;
using Jack.DataScience.Common;

namespace Jack.DataScience.Logging.Serilog
{
    public class SerilogModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.Register(context =>
            {
                var options = context.Resolve<SerilogOptions>();
                if(loggerConfiguration == null || logger == null)
                {
                    loggerConfiguration = new LoggerConfiguration();
                    loggerConfiguration.MinimumLevel.Debug();
                    loggerConfiguration.WriteTo.RollingFile("logs/{Date}.txt", (LogEventLevel) Enum.Parse(typeof(LogEventLevel), options.RollingFileLogEventLevel));
                    loggerConfiguration.WriteTo.ColoredConsole((LogEventLevel)Enum.Parse(typeof(LogEventLevel), options.ConsoleLogEventLevel));
                    logger = loggerConfiguration.CreateLogger();
                }
                return logger;
            });
            base.Load(builder);
        }

        private static LoggerConfiguration loggerConfiguration;
        private static ILogger logger;
    }
}
