using Autofac;
using Jack.DataScience.Common;
using Jack.DataScience.Data.AthenaClient;
using Jack.DataScience.Data.AWSAthena;
using Jack.DataScience.Storage.AWSS3;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;

namespace Jack.DataScience.Data.AthenaUI
{
    /// <summary>
    /// App.xaml 的交互逻辑
    /// </summary>
    public partial class App : Application
    {

        protected override void OnStartup(StartupEventArgs e)
        {
            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            AutoFacContainer autoFacContainer = new AutoFacContainer(environment);
            autoFacContainer.RegisterOptions<AWSAthenaOptions>();
            autoFacContainer.RegisterOptions<AWSS3Options>();
            autoFacContainer.RegisterOptions<AthenaClientOptions>();
            autoFacContainer.ContainerBuilder.RegisterModule<AWSAthenaModule>();
            autoFacContainer.ContainerBuilder.RegisterModule<AWSS3Module>();
            Services = autoFacContainer.ContainerBuilder.Build();
            base.OnStartup(e);
        }

        public IComponentContext Services { get; private set; }
    }
}
