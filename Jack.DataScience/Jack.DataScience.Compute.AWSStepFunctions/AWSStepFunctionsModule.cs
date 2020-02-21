using Autofac;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Compute.AWSStepFunctions
{
    public class AWSStepFunctionsModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSStepFunctionsAPI>();
            base.Load(builder);
        }
    }
}
