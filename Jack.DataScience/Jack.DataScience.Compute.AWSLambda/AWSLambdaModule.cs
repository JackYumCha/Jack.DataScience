using System;
using System.Collections.Generic;
using System.Text;
using Autofac;

namespace Jack.DataScience.Compute.AWSLambda
{
    public class AWSLambdaModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSLambdaAPI>();
            base.Load(builder);
        }
    }
}
