using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Authentication;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

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
                    EnabledSslProtocols = mongoOptions.SslProtocol // SslProtocols.Tls12
                };
            }
            mongoClient = new MongoClient(mongoClientSettings);
            MongoDatabase = mongoClient.GetDatabase(mongoOptions.Database);
        }

        public IMongoCollection<T> Collection<T>() where T : class
        {
            return MongoDatabase.GetCollection<T>(typeof(T).Name);
        }

        public IMongoCollection<T> Collection<TBase, T>() where T : TBase where TBase : class
        {
            return MongoDatabase.GetCollection<T>(typeof(TBase).Name);
        }

        public void DropCollection<T>() where T: class
        {
            MongoDatabase.DropCollection(typeof(T).Name);
        }
    }

    public static class MongoCollectionExtensions
    {

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

        public static BulkWriteResult<T> BulkReplaceEach<T>(
            this IMongoCollection<T> collection,
            IEnumerable<T> items
            )
            where T : DocumentBase
        {
            return collection.BulkWrite(
                items.Select(
                    item => new ReplaceOneModel<T>(Builders<T>.Filter.Where(document => document._id == item._id),item)
                    ));
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
