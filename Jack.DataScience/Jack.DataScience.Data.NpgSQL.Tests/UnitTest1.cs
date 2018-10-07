using System;
using Xunit;
using Jack.DataScience.Common;
using Autofac;
using Jack.DataScience.Data.NpgSQL;
using Jack.DataScience.DataTypes;

namespace Jack.DataScience.Data.NpgSQL.Tests
{
    public class UnitTest1
    {
        [Fact(DisplayName = "Create a Database")]
        public void CreateDatabase()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.ContainerBuilder.RegisterModule<NpgSQLModule>();

            var serviceContainer = container.ContainerBuilder.Build();
            var bootstrap = serviceContainer.Resolve<PostgreSQLBootstrap>();
            bootstrap.CreateDatabase();
        }

        [Fact(DisplayName ="Create a Table")]
        public void CreateTables()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.ContainerBuilder.RegisterModule<NpgSQLModule>();
            container.ContainerBuilder.RegisterModule<NpgSQLDataModule>();

            var serviceContainer = container.ContainerBuilder.Build();
            var npgsqlData = serviceContainer.Resolve<NpgSQLData>();
            npgsqlData.CreateTable<DemoEntity>();

        }

        [Fact(DisplayName = "Drop a Table")]
        public void DropTables()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.ContainerBuilder.RegisterModule<NpgSQLModule>();
            container.ContainerBuilder.RegisterModule<NpgSQLDataModule>();

            var serviceContainer = container.ContainerBuilder.Build();
            var npgsqlData = serviceContainer.Resolve<NpgSQLData>();
            npgsqlData.DropTable<DemoEntity>();

        }
    }
}
