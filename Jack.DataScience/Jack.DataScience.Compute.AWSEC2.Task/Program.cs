using System;
using System.Linq;
using Jack.DataScience.Common;
using Jack.DataScience.Compute.AWSEC2;
using Autofac;

namespace Jack.DataScience.Compute.AWSEC2.Task
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Execute AWS EC2 Job!");
            AutoFacContainer autoFacContainer = new AutoFacContainer();
            autoFacContainer.RegisterOptions<AWSEC2Options>();
            autoFacContainer.ContainerBuilder.RegisterModule<AWSEC2Module>();
            var services = autoFacContainer.ContainerBuilder.Build();
            var api = services.Resolve<AWSEC2API>();
            api.ExecuteJob().Wait();
        }
    }
}
