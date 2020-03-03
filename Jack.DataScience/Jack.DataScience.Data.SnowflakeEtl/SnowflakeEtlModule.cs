using Autofac;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.SnowflakeEtl
{
    public class SnowflakeEtlModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<SnowflakeEtlAPI>();
        }
    }
}
