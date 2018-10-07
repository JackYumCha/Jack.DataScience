using System;
using Xunit;
using Jack.DataScience.Common;
using Jack.DataScience.DataTypes;
using Autofac;
using ArangoDB.Client;

namespace Jack.DataScience.Data.Arango.Tests
{
    public class UnitTest1
    {
        [Fact(DisplayName = "Create Arango Collection")]
        public void CreateArangoCollection()
        {
            AutoFacContainer container = new AutoFacContainer();
            var serivcesContainer = container.ContainerBuilder.Build();

            var conn = serivcesContainer.Resolve<ArangoConnection>();
            var client = conn.CreateClient();

            client.CreateCollection(nameof(DemoEntity), type: CollectionType.Document);
        }

        [Fact(DisplayName = "Insert to Arango")]
        public void InsertToArango()
        {
            AutoFacContainer container = new AutoFacContainer();
            var serivcesContainer = container.ContainerBuilder.Build();

            var conn = serivcesContainer.Resolve<ArangoConnection>();
            var client = conn.CreateClient();

            client.Upsert<DemoEntity>(new DemoEntity()
            {
                _key = Guid.NewGuid().ToString(),
                Email = "a@b.com",
                Age = 28
            });
        }
    }
}
