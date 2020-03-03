using Autofac;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.Snowflake
{
    public class SnowflakeModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<SnowflakeAPI>();
        }
    }
}
