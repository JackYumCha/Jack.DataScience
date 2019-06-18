using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Authentication;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using Newtonsoft.Json;

namespace Jack.DataScience.Data.MongoDB
{
    public class MongoContext
    {
        private readonly MongoOptions mongoOptions;
        private readonly MongoClient mongoClient;
        public IMongoDatabase MongoDatabase { get; }

        public MongoContext(MongoOptions mongoOptions)
        {
            this.mongoOptions = mongoOptions;
            MongoClientSettings mongoClientSettings = MongoClientSettings.FromUrl(new MongoUrl(mongoOptions.Url));

            if (mongoOptions.SslProtocol != SslProtocols.None)
            {
                mongoClientSettings.SslSettings = new SslSettings()
                {
                    EnabledSslProtocols = mongoOptions.SslProtocol, // SslProtocols.Tls12
                    CheckCertificateRevocation = false,
                };
            }
            mongoClient = new MongoClient(mongoClientSettings);
            MongoDatabase = mongoClient.GetDatabase(mongoOptions.Database);
        }

        public List<string> ListCollections()
        {
            return MongoDatabase.ListCollectionNames().ToList();
        }

        public IMongoCollection<T> Collection<T>() where T : class
        {
            return MongoDatabase.GetCollection<T>(typeof(T).Name);
        }

        public IMongoCollection<BsonDocument> Collection(string name)
        {
            return MongoDatabase.GetCollection<BsonDocument>(name);
        }

        public IMongoCollection<MongoFunction> Functions()
        {
            return MongoDatabase.GetCollection<MongoFunction>("system.js");
        }

        public IMongoCollection<T> Collection<TBase, T>() where T : TBase where TBase : class
        {
            return MongoDatabase.GetCollection<T>(typeof(TBase).Name);
        }

        public IMongoCollection<BsonDocument> Collection(Type type)
        {
            return MongoDatabase.GetCollection<BsonDocument>(type.Name);
        }

        public void CreateEdgeIndices<T>() where T: EdgeBase
        {
            var indices = new List<CreateIndexModel<T>>()
            {
                new CreateIndexModel<T>(Builders<T>.IndexKeys.Hashed(t => t._from)),
                new CreateIndexModel<T>(Builders<T>.IndexKeys.Hashed(t => t._to)),
            };
            MongoDatabase.CreateCollection(typeof(T).Name);
            MongoDatabase.GetCollection<T>(typeof(T).Name).Indexes.CreateMany(indices);
        }
        public void CreateEdgeIndicesWithCollection<T>() where T : EdgeBase
        {
            var indices = new List<CreateIndexModel<T>>()
            {
                new CreateIndexModel<T>(Builders<T>.IndexKeys.Hashed(t => t._from)),
                new CreateIndexModel<T>(Builders<T>.IndexKeys.Hashed(t => t._to)),
                new CreateIndexModel<T>(Builders<T>.IndexKeys.Hashed(t => t._fromCollection)),
                new CreateIndexModel<T>(Builders<T>.IndexKeys.Hashed(t => t._toCollection)),
            };
            MongoDatabase.GetCollection<T>(typeof(T).Name).Indexes.CreateMany(indices);
        }

        public void DropCollection<T>() where T: class
        {
            MongoDatabase.DropCollection(typeof(T).Name);
        }

        public MongoFunctionResult<BsonDocument> InvokeFunction(string functionName, params object[] parameters)
        {
            var parameterJsons = new List<string>();
            foreach(var p in parameters)
            {
                parameterJsons.Add(p.ToJson());
            }
            var paramterString = $"{functionName}({string.Join(", ", parameterJsons)})" ;
            var escapedJson = paramterString.ToJson();
            var cmdJson = $"{{ eval: {escapedJson} }}";
            var cmd = new JsonCommand<BsonDocument>(cmdJson);
            var result = MongoDatabase.RunCommand(cmd);
            return BsonSerializer.Deserialize<MongoFunctionResult<BsonDocument>>(result);
        }

        public MongoFunctionResult<T> InvokeFunction<T>(string functionName, params object[] parameters)
        {
            var parameterJsons = new List<string>();
            foreach (var p in parameters)
            {
                parameterJsons.Add(p.ToJson(p.GetType()));
            }
            var paramterString = $"{functionName}({string.Join(", ", parameterJsons)})";
            var escapedJson = paramterString.ToJson();
            var cmdJson = $"{{ eval: {escapedJson} }}";
            var cmd = new JsonCommand<BsonDocument>(cmdJson);
            var result = MongoDatabase.RunCommand(cmd);
            return BsonSerializer.Deserialize<MongoFunctionResult<T>>(result);
        }

        public List<string> ClearExtensions()
        {
            var colFunctions = Functions();
            var functions = colFunctions.AsQueryable().Select(f => f._id).ToList();
            colFunctions.DeleteMany(Builders<MongoFunction>.Filter.In(mf => mf._id, functions));
            return functions;
        }

        public List<string> LoadExtensions()
        {
            // find js files in "./Functions/compiled/*
            Regex rgxFunctionName = new Regex(@"^\s*function\s+(\w+)");
            List<string> results = new List<string>();
            var rootDir = new DirectoryInfo($"{AppContext.BaseDirectory}/Functions/compiled");
            //var jsFunctions = Directory.GetFiles($"{AppContext.BaseDirectory}/Functions/compiled", "*.js");
            var colFunctions = Functions();
            foreach(var functionFile in rootDir.FindAllJavaScripts())
            {
                var jsCode = File.ReadAllText(functionFile.FullName);
                var match = rgxFunctionName.Match(jsCode);
                if (match.Success)
                {
                    var name = match.Groups[1].Value;
                    jsCode = rgxFunctionName.Replace(jsCode, "function ");
                    colFunctions.UpsertOne(new MongoFunction()
                    {
                        _id = name,
                        value = new BsonJavaScript(jsCode)
                    });
                    results.Add(name);
                }
            }
            return results;
        }
    }

    public static class MongoCollectionExtensions
    {

        public static TDocument ById<TDocument>(this MongoContext mongoContext, string _id) where TDocument: DocumentBase
        {
            return mongoContext.Collection<TDocument>().GetOneById(_id);
        }

        internal static IEnumerable<FileInfo> FindAllJavaScripts(this DirectoryInfo rootFolder)
        {
            foreach(var js in rootFolder.GetFiles("*.js"))
            {
                yield return js;
            }
            foreach(var js in rootFolder.GetDirectories().SelectMany(dir => dir.FindAllJavaScripts()))
            {
                yield return js;
            }
        }

        private static char[] digitLetterChars = "abcdefghijgklmnopqrstuvwxyz0123456789".ToCharArray();
        private static char[] digitLetterCaseSensitiveChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijgklmnopqrstuvwxyz0123456789".ToCharArray();
        private static char[] letterChars = "abcdefghijgklmnopqrstuvwxyz".ToCharArray();
        private static char[] digitChars = "0123456789".ToCharArray();
        private static Random random = new Random();
        public static T RandomDigitLetterCaseSenstiveId<T>(this T document, int length, string prefix = "") where T : DocumentBase
        {
            var id = "";
            while (id.Length < length)
            {
                id += digitLetterCaseSensitiveChars[random.Next(digitLetterCaseSensitiveChars.Length)];
            }
            document._id = prefix + id;
            return document;
        }
        public static T RandomDigitLetterId<T>(this T document, int length, string prefix = "") where T: DocumentBase
        {
            var id = "";
            while(id.Length< length)
            {
                id += digitLetterChars[random.Next(digitLetterChars.Length)];
            }
            document._id = prefix + id;
            return document;
        }
        public static T RandomDigitId<T>(this T document, int length, string prefix = "") where T : DocumentBase
        {
            var id = "";
            while (id.Length < length)
            {
                id += digitChars[random.Next(digitChars.Length)];
            }
            document._id = prefix + id;
            return document;
        }
        public static T RandomLetterId<T>(this T document, int length, string prefix = "") where T : DocumentBase
        {
            var id = "";
            while (id.Length < length)
            {
                id += letterChars[random.Next(letterChars.Length)];
            }
            document._id = prefix + id;
            return document;
        }
        public static T RandomNumberId<T>(this T document, int max, string prefix = "") where T : DocumentBase
        {
            document._id = prefix + random.Next(max).ToString();
            return document;
        }
        //public static ReplaceOneResult Find<T, TProjection>(this IMongoCollection<T> collection,
        //    Func<FilterDefinitionBuilder<T>, FilterDefinition<T>> filterBuilder,
        //    Func<ProjectionDefinitionBuilder<TProjection>, ProjectionDefinition<TProjection>> projectionBuilder
        //    ) where T : DocumentBase
        //{
        //    return collection.FindSync(filterBuilder(Builders<T>.Filter), new FindOptions<T, TProjection>()
        //    {
        //        Projection = projectionBuilder(Builders<T>.Projection. )
        //    };
        //}
        public static DeleteResult DeleteOneById<T>(this IMongoCollection<T> collection, string id) where T : DocumentBase
        {
            return collection.DeleteOne(Builders<T>.Filter.Where(f => f._id == id));
        }

        public static T GetOneById<T>(this IMongoCollection<T> collection, string id) where T : DocumentBase
        {
            return collection.Find(Builders<T>.Filter.Where(f => f._id == id)).FirstOrDefault();
        }

        public static List<T> GetByIds<T>(this IMongoCollection<T> collection, IEnumerable<string> ids) where T : DocumentBase
        {
            return collection.Find(Builders<T>.Filter.In(doc => doc._id, ids)).ToList();
        }

        public static T InsertOneWithIdBuilder<T>(this IMongoCollection<T> collection, T document, Expression<Action<T>> expression) where T : DocumentBase
        {
            bool success = false;
            var builder = expression.Compile();
            List<string> ids = new List<string>();
            while (!success)
            {
                try
                {
                    builder.Invoke(document);
                    ids.Add(document._id);
                    while(ids.Count > 10)
                    {
                        ids.RemoveAt(0);
                    }
                    if (ids.Count >= 10)
                    {
                        var first = ids[0];
                        if(ids.All(id => id == first))
                        {
                            throw new Exception($"Id Generator has Create 10 same Id '{first}'. Please check your Id generator.");
                        }
                    }
                    collection.InsertOne(document);
                    success = true;
                }
                catch(MongoWriteException ex)
                {
                    if (!ex.Message.Contains("E11000"))
                    {
                        throw ex;
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                };
            }
            return document;
        }

        public static bool HasId<T>(this IMongoCollection<T> collection, string id) where T : DocumentBase
        {
            return collection.AsQueryable().Any(f => f._id == id);
        }

        public static ReplaceOneResult ReplaceOne<T>(this IMongoCollection<T> collection, T item) where T : DocumentBase
        {
            return collection.ReplaceOne(Builders<T>.Filter.Where(f => f._id == item._id), item);
        }

        public static ReplaceOneResult UpsertOne<T>(this IMongoCollection<T> collection, T item, string newId) where T : DocumentBase
        {
            if(item._id != newId)
            {
                // remove old
                collection.DeleteOneById(item._id);
                item._id = newId;
            }
            return collection.ReplaceOne(Builders<T>.Filter.Where(f => f._id == item._id), item, new UpdateOptions() { IsUpsert = true });
        }

        public static ReplaceOneResult UpsertOne<T>(this IMongoCollection<T> collection, T item) where T : DocumentBase
        {
            return collection.ReplaceOne(Builders<T>.Filter.Where(f => f._id == item._id), item, new UpdateOptions() { IsUpsert = true });
        }

        public static DeleteResult DeleteOne<T>(this IMongoCollection<T> collection, T item) where T : DocumentBase
        {
            return collection.DeleteOne(Builders<T>.Filter.Where(f => f._id == item._id));
        }

        public static UpdateResult UpdateWhereMany<T>(
            this IMongoCollection<T> collection, 
            Expression<Func<T, bool>> fieldSelector, Func<UpdateDefinitionBuilder<T>, UpdateDefinition<T>> updater)
            where T: DocumentBase
        {
            return collection.UpdateMany(Builders<T>.Filter.Where(fieldSelector), updater(Builders<T>.Update));
        }

        public static async Task<UpdateResult> UpdateWhereManyAsync<T>(
                    this IMongoCollection<T> collection,
                    Expression<Func<T, bool>> condition, Func<UpdateDefinitionBuilder<T>, UpdateDefinition<T>> updater)
                    where T : DocumentBase
        {
            return await collection.UpdateManyAsync(Builders<T>.Filter.Where(condition), updater(Builders<T>.Update));
        }

        public static UpdateResult UpdateWhereOne<T>(
    this IMongoCollection<T> collection,
    Expression<Func<T, bool>> fieldSelector, Func<UpdateDefinitionBuilder<T>, UpdateDefinition<T>> updater)
    where T : DocumentBase
        {
            return collection.UpdateOne(Builders<T>.Filter.Where(fieldSelector), updater(Builders<T>.Update));
        }

        public static async Task<UpdateResult> UpdateWhereOneAsync<T>(
                    this IMongoCollection<T> collection,
                    Expression<Func<T, bool>> condition, Func<UpdateDefinitionBuilder<T>, UpdateDefinition<T>> updater)
                    where T : DocumentBase
        {
            return await collection.UpdateOneAsync(Builders<T>.Filter.Where(condition), updater(Builders<T>.Update));
        }

        public static async Task<BulkWriteResult<T>> BulkReplaceEachAsync<T>(
            this IMongoCollection<T> collection, 
            IEnumerable<T> items
            )
            where T: DocumentBase
        {
            return await collection.BulkWriteAsync(
                items.Select(
                    item => new ReplaceOneModel<T>(Builders<T>.Filter.Where(document => document._id == item._id),
                item)));
        }

        public static long BulkReplaceEach<T>(
            this IMongoCollection<T> collection,
            IEnumerable<T> items
            )
            where T : DocumentBase
        {
            if (!items.Any())
            {
                return 0L;
            }
            return collection.BulkWrite(
                items.Select(
                    item => new ReplaceOneModel<T>(Builders<T>.Filter.Where(document => document._id == item._id),item)
                    )).ModifiedCount;
        }

        public static IEnumerable<BulkWriteUpsert> BulkInsertEach<T>(
          this IMongoCollection<T> collection,
          IEnumerable<T> items
          )
          where T : DocumentBase
        {
            if (!items.Any())
            {
                return new List<BulkWriteUpsert>();
            }
            return collection.BulkWrite(
                items.Select(
                    item => new InsertOneModel<T>(item)
                    ), new BulkWriteOptions()
                    {
                        IsOrdered = false
                    }).Upserts;
        }

        public static BulkWriteResult<T> BulkUpdateEach<T>(
            this IMongoCollection<T> collection,
            IEnumerable<T> items,
            Func<UpdateDefinitionBuilder<T>, T, UpdateDefinition<T>> updater
            )
            where T : DocumentBase
        {
            return collection.BulkWrite(
                items.Select(
                    item => new UpdateOneModel<T>(Builders<T>.Filter.Where(document => document._id == item._id), updater(Builders<T>.Update, item))
                    ));
        }

        public static async Task<BulkWriteResult<T>> BulkUpdateEachAsync<T>(
            this IMongoCollection<T> collection,
            IEnumerable<T> items,
            Func<UpdateDefinitionBuilder<T>, T, UpdateDefinition<T>> updater
            )
            where T : DocumentBase
        {
            return await collection.BulkWriteAsync(
                items.Select(
                    item => new UpdateOneModel<T>(Builders<T>.Filter.Where(document => document._id == item._id), updater(Builders<T>.Update, item))
                    ));
        }
    }
}
