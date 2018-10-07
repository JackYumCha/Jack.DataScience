using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using MongoDB.Driver;
using Jack.DataScience.Common;
using Autofac;
using MongoDB.Bson.Serialization.Attributes;
using Jack.DataScience.DataTypes;

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

            var testDB = client.GetDatabase("TestDB2");

            // testDB.CreateCollection(nameof(DemoEntity));

            testDB.GetCollection<DemoEntity>(nameof(DemoEntity)).InsertMany(
                new List<DemoEntity>()
                {
                   new DemoEntity(){ Age = 28, Email = "a@b.com", Name="Mongo"}
                });
        }
    }
}
