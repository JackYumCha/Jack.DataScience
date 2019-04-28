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
            //builder.Register(context =>
            //{
            //    var options = context.Resolve<SerilogOptions>();
            //    if(loggerConfiguration == null || logger == null)
            //    {
            //        loggerConfiguration = new LoggerConfiguration();
            //        loggerConfiguration.MinimumLevel.Debug();
            //        var rollingFileEventLevel = (LogEventLevel)Enum.Parse(typeof(LogEventLevel), options.RollingFileLogEventLevel);
            //        loggerConfiguration.WriteTo.RollingFile("logs/{Date}.txt", rollingFileEventLevel);
            //        var coloredConsoleEventLevel = (LogEventLevel)Enum.Parse(typeof(LogEventLevel), options.ConsoleLogEventLevel);
            //        loggerConfiguration.WriteTo.ColoredConsole(coloredConsoleEventLevel);
            //        logger = loggerConfiguration.CreateLogger();
            //    }
            //    return logger;
            //});

            builder.RegisterInstance(GetLogger());
            base.Load(builder);
        }

        private ILogger GetLogger()
        {
            if (loggerConfiguration == null || logger == null)
            {
                loggerConfiguration = new LoggerConfiguration();
                loggerConfiguration.MinimumLevel.Debug();
                LogEventLevel rollingFileEventLevel = LogEventLevel.Debug; // (LogEventLevel)Enum.Parse(typeof(LogEventLevel), options.RollingFileLogEventLevel);
                loggerConfiguration.WriteTo.RollingFile("logs/{Date}.txt", rollingFileEventLevel);
                LogEventLevel coloredConsoleEventLevel = LogEventLevel.Debug;  // (LogEventLevel)Enum.Parse(typeof(LogEventLevel), options.ConsoleLogEventLevel);
                loggerConfiguration.WriteTo.ColoredConsole(coloredConsoleEventLevel);
                logger = loggerConfiguration.CreateLogger();
            }
            return logger;
        }

        private static LoggerConfiguration loggerConfiguration;
        private static ILogger logger;
    }
}
