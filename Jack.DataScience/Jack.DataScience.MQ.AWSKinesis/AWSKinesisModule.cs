using Autofac;
using System;

namespace Jack.DataScience.MQ.AWSKenesis
{
    public class AWSKinesisModule : Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSKinesisAPI>();  
        }
    }
}
