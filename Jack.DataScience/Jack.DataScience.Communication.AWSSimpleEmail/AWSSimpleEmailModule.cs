using System;
using System.Collections.Generic;
using System.Text;
using Autofac;

namespace Jack.DataScience.Communication.AWSSimpleEmail
{
    public class AWSSimpleEmailModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSSimpleEmailAPI>();
            base.Load(builder);
        }
    }
}
