using Autofac;
using System;

namespace Jack.DataScience.Data.AzureTableStorage
{
    public class AzureTableStorageModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AzureTableStorageAPI>();
        }
    }
}
