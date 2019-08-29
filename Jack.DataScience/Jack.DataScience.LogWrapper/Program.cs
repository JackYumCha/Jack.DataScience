using Autofac;
using Jack.DataScience.Common;
using Jack.DataScience.ConsoleExtensions;
using Jack.DataScience.Data.MongoDB;
using MongoDB.Driver;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Jack.DataScience.LogWrapper
{
    class Program
    {
        static void Main(string[] args)
        {
            var DotNetLog = args.GetParameter("--dot-net-log");
            if (DotNetLog == null) DotNetLog = Environment.GetEnvironmentVariable("DOTNETLOG");
            AutoFacContainer autoFacContainer = new AutoFacContainer(DotNetLog);
            autoFacContainer.RegisterOptions<LogWrapperOptions>();
            var services = autoFacContainer.ContainerBuilder.Build();
            var options = services.Resolve<LogWrapperOptions>();

            MongoContext mongoContext = new MongoContext(new MongoOptions()
            {
                Url = options.MongoDBUrl,
                SslProtocol = options.SslProtocol,
                Database = options.Database
            });

            var collectionName = options.CollectionName;
            var useTime = args.HasParameter("--use-time");
            var useGuid = args.HasParameter("--use-guid");
            if (useTime) collectionName += $"_{DateTime.UtcNow.ToString("yyyyMMddHHmmss")}";
            if (useGuid) collectionName += $"_{Guid.NewGuid().ToString().Replace("-", "")}";
            collectionLogMessage = mongoContext.MongoDatabase.GetCollection<LogMessage>(collectionName);
            collectionLogMessage.InsertOne(new LogMessage()
            {
                Timestamp = DateTime.UtcNow,
                LogLevel = "Wrap",
                Message = $"Process Start: {options.Command} {(options.Arguments == null ? "" : string.Join(" ", options.Arguments))}"
            });

            ProcessStartInfo psi = new ProcessStartInfo()
            {
                FileName = options.Command,
                RedirectStandardError = true,
                RedirectStandardOutput = true
            };

            if(options.Arguments != null)
                options.Arguments.ForEach(value => psi.ArgumentList.Add(value));

            var process = Process.Start(psi);
            process.OutputDataReceived += OutputReceived;
            process.ErrorDataReceived += ErrorReceived;
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            process.WaitForExit();

            collectionLogMessage.InsertOne(new LogMessage()
            {
                Timestamp = DateTime.UtcNow,
                LogLevel = "Wrap",
                Message = $"Process End: {options.Command} {(options.Arguments == null ? "" : string.Join(" ", options.Arguments))}"
            });

            Thread.Sleep(2000);
        }

        private static IMongoCollection<LogMessage> collectionLogMessage;

        public static void OutputReceived(object sender, DataReceivedEventArgs e)
        {
            Console.WriteLine(e.Data);
            collectionLogMessage.InsertOne(new LogMessage()
            {
                Timestamp = DateTime.UtcNow,
                LogLevel = "Log",
                Message = e.Data
            });
        }

        public static void ErrorReceived(object sender, DataReceivedEventArgs e)
        {
            Console.Error.WriteLine(e.Data);
            collectionLogMessage.InsertOne(new LogMessage()
            {
                Timestamp = DateTime.UtcNow,
                LogLevel = "Error",
                Message = e.Data
            });
        }
    }
}
