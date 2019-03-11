using System;
using System.Collections.Generic;
using System.Text;
using Autofac;
namespace Jack.DataScience.Communication.AWSSNS
{
    public class SNSModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSSNSAPI>();
            base.Load(builder);
        }
    }
}
