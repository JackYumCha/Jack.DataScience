using System;
using System.Collections.Generic;
using System.Text;
using Autofac;

namespace Jack.DataScience.Communication.SendGrid
{
    public class SendGridModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<SendGridAPI>();
            base.Load(builder);
        }
    }
}
