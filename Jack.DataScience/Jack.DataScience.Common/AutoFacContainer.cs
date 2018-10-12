using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;
using Autofac;
using Npgsql.Logging;

namespace Jack.DataScience.Common
{
    public class AutoFacContainer
    {
        public AutoFacContainer(string environment = null)
        {
            var configBuilder = new ConfigurationBuilder();
            configBuilder.AddJsonFile($"{AppContext.BaseDirectory}/appsettings{((environment == null) ? "" : $".{environment}")}.json");
            Configuration = configBuilder.Build();

            ContainerBuilder = new ContainerBuilder();

            RegisterOptions<SerilogOptions>();
            RegisterOptions<SqlServerOptions>();
            RegisterOptions<JwtSecretOptions>(); 
        }

        public IConfiguration Configuration { get; set; }

        public ContainerBuilder ContainerBuilder { get; set; }

        public void RegisterOptions<T>() where T: class
        {
            ContainerBuilder.RegisterOptions<T>(Configuration);
        }
    }

    public static class AutoFaceExtensions
    {
        public static void RegisterOptions<T>(this ContainerBuilder containerBuilder, IConfiguration configuration) where T: class
        {
            Type type = typeof(T);
            var section = configuration.GetSection(type.Name);
            if(section != null && section.Exists())
            {
                T options = section.Get<T>();
                containerBuilder.RegisterInstance(options);
            }
        }
    }
}
