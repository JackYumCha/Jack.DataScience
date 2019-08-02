using System;
using Serilog;
using Autofac;
using Serilog.Sinks.AwsCloudWatch;
using Serilog.Sinks;
using Serilog.Sinks.SystemConsole;
using Serilog.Formatting.Json;
using Jack.DataScience.Common;
using Amazon.CloudWatchLogs;
using Amazon.Runtime;
using Amazon;
using Serilog.Events;

namespace Jack.DataScience.Logging.AWSCloudWatch
{
    /// <summary>
    /// this will register GenericLogger for the cloud watch logs
    /// </summary>
    public class AWSCloudWatchSerilogModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.Register(context =>
            {
                var awsCloudWatchOptions = context.Resolve<AWSCloudWatchOptions>();
                BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsCloudWatchOptions.Key, awsCloudWatchOptions.Secret);
                return new AmazonCloudWatchLogsClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsCloudWatchOptions.Region));
            });

            builder.Register<ILogger>(context =>
            {
                var options = context.Resolve<CloudWatchSinkOptions>();
                options.Period = TimeSpan.FromSeconds(10);
                options.LogStreamNameProvider = new DefaultLogStreamProvider();
                options.TextFormatter = new JsonFormatter();
                var client = context.Resolve<AmazonCloudWatchLogsClient>();
                Log.Logger = new LoggerConfiguration()
                    .WriteTo.ColoredConsole(LogEventLevel.Verbose)
                    .WriteTo.AmazonCloudWatch(options, client)
                    .CreateLogger();
                return Log.Logger;
            });

            builder.Register(context =>
            {
                var logger =  context.Resolve<ILogger>();
                var genericLogger = new GenericLogger()
                {
                    Info = value => logger.Information(value),
                    Verbose = value => logger.Verbose(value),
                    Error = value => logger.Error(value),
                    Warn = value => logger.Warning(value),
                    Log = value => logger.Debug(value),
                };
                Common.Logging.Console.Logger = genericLogger;
                return genericLogger;
            });

        }
    }
}
