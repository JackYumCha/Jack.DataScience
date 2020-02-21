using Autofac;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.AWSAsync
{
    public class AWSAsyncModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AsyncLogic>();
            base.Load(builder);
        }
    }
}
