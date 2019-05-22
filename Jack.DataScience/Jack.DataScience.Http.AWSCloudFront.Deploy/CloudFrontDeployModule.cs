using Autofac;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Http.AWSCloudFront.Deploy
{
    public class CloudFrontDeployModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<CloudFrontDeployAPI>();
        }
    }
}
