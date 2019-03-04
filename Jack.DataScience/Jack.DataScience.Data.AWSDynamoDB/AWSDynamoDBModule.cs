using System;
using System.Collections.Generic;
using System.Text;
using Autofac;

namespace Jack.DataScience.Data.AWSDynamoDB
{
    public class AWSDynamoDBModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<AWSDynamoAPI>();
            base.Load(builder);
        }
    }
}
