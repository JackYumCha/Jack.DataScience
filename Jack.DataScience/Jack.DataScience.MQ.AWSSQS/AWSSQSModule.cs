using Autofac;
using System;

namespace Jack.DataScience.MQ.AWSSQS
{
    public class AWSSQSModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSSQSAPI>();
        }
    }
}
