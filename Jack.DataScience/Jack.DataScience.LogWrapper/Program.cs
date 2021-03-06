﻿using Autofac;
using Jack.DataScience.Common;
using Jack.DataScience.ConsoleExtensions;
using Jack.DataScience.Data.MongoDB;
using Jack.DataScience.ProcessExtensions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
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
            if(!mongoContext.ListCollections().Any(name => name == collectionName))
            {
                if (options.Capped)
                {
                    mongoContext.MongoDatabase.CreateCollection(collectionName, new CreateCollectionOptions()
                    {
                        Capped = true,
                        MaxDocuments = options.MaxDocuments,
                        MaxSize = options.MaxSize
                    });
                }
                else
                {
                    mongoContext.MongoDatabase.CreateCollection(collectionName);
                }
            }
            IMongoCollection<LogMessage> collectionLogMessage = mongoContext.MongoDatabase.GetCollection<LogMessage>(collectionName);
            collectionLogMessage.InsertOne(new LogMessage()
            {
                Timestamp = DateTime.UtcNow,
                LogLevel = "Wrap",
                Message = $"Process Start: {options.Command} {(options.Arguments == null ? "" : string.Join(" ", options.Arguments))}"
            });

            ProcessExecutor processExecutor = new ProcessExecutor(options.Command);
            processExecutor.AddArguments(options.Arguments);

            processExecutor.StandardOutput
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .Subscribe(
                line =>
                {
                    Console.WriteLine(line);
                    InsertLog(collectionLogMessage, "Log", line);
                });

            processExecutor.StandardError
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .Subscribe(
                line =>
                {
                    Console.Error.WriteLine(line);
                    InsertLog(collectionLogMessage, "Error", line);
                });

            processExecutor.Execute();

            processExecutor.Dispose();

            collectionLogMessage.InsertOne(new LogMessage()
            {
                Timestamp = DateTime.UtcNow,
                LogLevel = "Wrap",
                Message = $"Process End: {options.Command} {(options.Arguments == null ? "" : string.Join(" ", options.Arguments))}"
            });

            Thread.Sleep(2000);
        }

        static string jsonLogMessageMatch = "{\"JsonLogType\":\"[JsonLogType]\",";
        public static void InsertLog(IMongoCollection<LogMessage> collection, string type, string line)
        {
            string message = "";
            BsonDocument bsonDocument = null;
            if (line.StartsWith(jsonLogMessageMatch))
            {
                JsonLogMessage log = JsonConvert.DeserializeObject<JsonLogMessage>(line);
                message = log.Message;
                if(!string.IsNullOrWhiteSpace(log.Json))
                {
                    try
                    {
                        bsonDocument = BsonSerializer.Deserialize<BsonDocument>(log.Json);
                    }
                    catch(Exception ex) { }
                }
            }
            else
            {
                message = line;
            }
            collection.InsertOne(new LogMessage()
            {
                Timestamp = DateTime.UtcNow,
                LogLevel = "Error",
                Message = message,
                Value = bsonDocument,
            });
        }
    }
}
