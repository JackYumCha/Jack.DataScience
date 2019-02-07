using System;
using System.Collections.Generic;
using System.Text;
using Autofac;

namespace Jack.DataScience.Automation.TwilioSMS
{
    public class TwilioModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<TwilioSender>();
            base.Load(builder);
        }
    }
}
