using System;
using System.Diagnostics;
using System.Collections.Generic;
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
                    YearBirth = 2001,
                    IsMale = true
                });
                context.SaveChanges();
            }
        }


        [Fact(DisplayName = "Insert Random Users")]
        public void InsertRandomUsers()
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
                Random rnd = new Random(DateTime.Now.Millisecond);
                for (int i = 0; i<100; i++)
                {
                    context.Users.Add(new User()
                    {
                        Name = $"User {i}",
                        YearBirth = (int)(1985 + rnd.NextDouble() * 30),
                        IsMale = rnd.NextDouble() > 0.5d
                    });

                    context.Cars.Add(new Car()
                    {
                        Year = (int)(1985 + rnd.NextDouble() * 30),
                        Made = rnd.NextDouble() > 5d ? "Toyota":"BMW"
                    });
                }
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
                // select * from "Users" where IsMale = true and Age > 80
                var users = context.Users
                    //.Where(u => u.IsMale == true && u.YearBirth > 2005)
                    //.GroupBy(u => u.YearBirth)
                    .ToList();

                var cars = context.Cars.ToList();

                var avgCarYear = users//.AsParallel()
                    .Select(user => cars.Where(car => car.Year == user.YearBirth && car.Id % 2 == 1))
                    .Aggregate(new List<Car>(), (seed, _cars) =>
                    {
                        seed.AddRange(_cars);
                        return seed;
                    })
                    .Average(car => car.Year);

                Debugger.Break();
                context.SaveChanges();
            }
        }

    private List<Car> agg(List<Car> seed, IEnumerable<Car> _cars) 
    {
        seed.AddRange(_cars);
        return seed;
    }

    /*
     -- select * from "Cars"
-- select * from "Users"
-- select concat("IsMale","Id" % 2) as gender, avg("YearBirth") from "Users" group by gender
select 	Sum(case when "Cars"."Id" % 2 = 1 then "Year" Else 0 End)/Sum(case when "Cars"."Id" % 2 = 1 then 1 Else 0 End),
    Sum(case when "Year" % 2 = 0 then "Year" Else 0 End)/Sum(case when "Year" % 2 = 0 then 1 Else 0 End)
from "Users"
Left Join "Cars" on
"Cars"."Year" = "Users"."YearBirth"
     */
}
}
