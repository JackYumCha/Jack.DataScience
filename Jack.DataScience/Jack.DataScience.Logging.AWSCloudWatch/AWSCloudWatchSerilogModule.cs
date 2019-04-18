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
                var options = context.Resolve<CloudWatchSinkOptions>();
                options.Period = TimeSpan.FromSeconds(10);
                options.LogStreamNameProvider = new DefaultLogStreamProvider();
                options.TextFormatter = new JsonFormatter();
                var awsCloudWatchOptions = context.Resolve<AWSCloudWatchOptions>();
                BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsCloudWatchOptions.Key, awsCloudWatchOptions.Secret);
                var client = new AmazonCloudWatchLogsClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsCloudWatchOptions.Region));

                Log.Logger = new LoggerConfiguration()
                    .WriteTo.ColoredConsole(LogEventLevel.Verbose)
                    .WriteTo.AmazonCloudWatch(options, client)
                    .CreateLogger();

                ILogger logger = Log.Logger;

                return new GenericLogger()
                {
                    Info = value => logger.Information(value),
                    Verbose = value => logger.Verbose(value),
                    Error = value => logger.Error(value),
                    Warn = value => logger.Warning(value),
                    Log = value => logger.Debug(value),
                };
            });
        }
    }
}
