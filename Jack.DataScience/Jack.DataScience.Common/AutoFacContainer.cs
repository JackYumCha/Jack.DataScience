using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;
using Autofac;
using Newtonsoft.Json;
using System.IO;

namespace Jack.DataScience.Common
{
    public class AutoFacContainer
    {
        public AutoFacContainer(string environment = null)
        {
            var configBuilder = new ConfigurationBuilder();
            var appsettingsFile = $"{AppContext.BaseDirectory}/appsettings{(string.IsNullOrWhiteSpace(environment) ? "" : $".{environment}")}.json";
            if (!File.Exists(appsettingsFile))
            {
                throw new Exception($"appsettings file was not found at: {appsettingsFile}");
            }
            configBuilder.AddJsonFile(appsettingsFile);
            Configuration = configBuilder.Build();
            ContainerBuilder = new ContainerBuilder();
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
