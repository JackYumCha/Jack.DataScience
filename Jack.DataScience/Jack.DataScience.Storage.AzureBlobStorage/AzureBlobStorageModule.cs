using System;
using System.Collections.Generic;
using System.Text;
using Autofac;

namespace Jack.DataScience.Storage.AzureBlobStorage
{
    public class AzureBlobStorageModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AzureBlobStorageAPI>();
            base.Load(builder);
        }
    }
}
