using System;
using Xunit;
using Jack.DataScience.Common;
using Autofac;
using Jack.DataScience.Data.MongoDB;

namespace Jack.DataScience.Data.MongoDB.Tests
{
    public class BootstrapTests
    {
        [Fact(DisplayName = "Create MongoDB Instance")]
        public void CreateDatabase()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.ContainerBuilder.RegisterModule<MongoModule>();
            var serivcesContainer = container.ContainerBuilder.Build();

            var mongoBootstrap = serivcesContainer.Resolve<MongoBootstrap>();

            mongoBootstrap.CreateDatabase();
        }

        [Fact(DisplayName = "Delete MongoDB Instance")]
        public void DeleteDatabase()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.ContainerBuilder.RegisterModule<MongoModule>();
            var serivcesContainer = container.ContainerBuilder.Build();

            var mongoBootstrap = serivcesContainer.Resolve<MongoBootstrap>();

            mongoBootstrap.DeleteDatabase();
        }
    }
}
