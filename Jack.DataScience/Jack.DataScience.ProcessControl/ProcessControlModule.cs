using Autofac;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.ProcessControl
{
    public class ProcessControlModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<ProcessControlAPI>();
        }
    }
}
