using System;
using System.Collections.Generic;
using System.Text;
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
            mongoClient = new MongoClient(mongoOptions.Url);
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
    }
}
