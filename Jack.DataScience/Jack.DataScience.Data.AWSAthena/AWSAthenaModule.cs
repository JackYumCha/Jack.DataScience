using System;
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon;
using Autofac;

namespace Jack.DataScience.Data.AWSAthena
{
    public class AWSAthenaModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSAthenaAPI>();
            base.Load(builder);
        }
    }
}
