using System;
using System.Diagnostics;
using System.Linq;
using Jack.DataScience.Common;
using Jack.DataScience.Data.NpgSQL.EF;
using Microsoft.EntityFrameworkCore;
using Npgsql.EntityFrameworkCore;
using Autofac;
using Xunit;

namespace Jack.DataScience.Data.NpgSQL.EF.Tests
{
    public class CodeFirstTests
    {
        [Fact(DisplayName = "Setup and Insert Object")]
        public void SetupAndInsertObject()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<PostgreSQLEFOptions>();

            container.ContainerBuilder.Register(context =>
            {
                var options = context.Resolve<PostgreSQLEFOptions>();
                var builder = new DbContextOptionsBuilder<CodeFirstContext>();
                builder.UseNpgsql(options.ConnectionString);
                return builder.Options;
            });

            container.ContainerBuilder.RegisterType<CodeFirstContext>();

            var serivcesContainer = container.ContainerBuilder.Build();

            using (CodeFirstContext context = serivcesContainer.Resolve<CodeFirstContext>())
            {
                context.Database.EnsureCreated();
                context.Users.Add(new User()
                {
                    Name = "Tom",
                    Age = 99,
                    IsMale = true
                });
                context.SaveChanges();
            }
        }

        [Fact(DisplayName = "Query Object")]
        public void QueryObject()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<PostgreSQLEFOptions>();

            container.ContainerBuilder.Register(context =>
            {
                var options = context.Resolve<PostgreSQLEFOptions>();
                var builder = new DbContextOptionsBuilder<CodeFirstContext>();
                builder.UseNpgsql(options.ConnectionString);
                return builder.Options;
            });

            container.ContainerBuilder.RegisterType<CodeFirstContext>();

            var serivcesContainer = container.ContainerBuilder.Build();

            using (CodeFirstContext context = serivcesContainer.Resolve<CodeFirstContext>())
            {
                context.Database.EnsureCreated();
                var users = context.Users.Where(u => u.IsMale == true && u.Age > 80).ToList();
                Debugger.Break();
                context.SaveChanges();
            }
        }

    }
}
