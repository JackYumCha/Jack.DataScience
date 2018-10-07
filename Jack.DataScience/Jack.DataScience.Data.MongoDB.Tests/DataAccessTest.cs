using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using MongoDB.Driver;
using Jack.DataScience.Common;
using Autofac;
using MongoDB.Bson.Serialization.Attributes;

namespace Jack.DataScience.Data.MongoDB.Tests
{
    public class DataAccessTest
    {

        [Fact(DisplayName = "Write Data To MongoDB")]
        public void WriteDataToMongoDB()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.ContainerBuilder.RegisterModule<MongoModule>();
            var serivcesContainer = container.ContainerBuilder.Build();
            
            MongoClient client = serivcesContainer.Resolve<MongoClient>();

            var testDB = client.GetDatabase("TestDB");

            testDB.CreateCollection(nameof(Demo));

            testDB.GetCollection<Demo>(nameof(Demo)).InsertMany(
                new List<Demo>()
                {
                   new Demo(){ Age = 28, Email = "a@b.com", Name="Mongo"}
                });
        }
    }

    public class Demo
    {
        [BsonIdAttribute]
        public string _id { get; set; }

        public string Name { get; set; }
        public int Age { get; set; }
        public string Email { get; set; }
    }
}
