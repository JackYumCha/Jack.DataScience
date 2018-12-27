using System;
using Autofac;

namespace Jack.DataScience.Compute.AWSEC2
{
    public class AWSEC2Module: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSEC2API>();
            base.Load(builder);
        }
    }
}
