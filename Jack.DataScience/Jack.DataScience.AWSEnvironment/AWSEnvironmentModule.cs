using Amazon.Runtime;
using Autofac;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.AWSEnvironment
{
    public class AWSEnvironmentModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            var credentials = CredentialResolver.ResolveFromAWSEnvironemt();
            switch (credentials.Mode)
            {
                case CredentialModeEnum.AccessSecretRegion:
                    builder.Register((IComponentContext context) => credentials.CreateBasicAWSCredentials());
                    builder.Register((IComponentContext context) => credentials.CreateRegionEndpoint());
                    break;
                case CredentialModeEnum.AccessSecretToken:
                    builder.Register((IComponentContext context) => credentials.CreateSessionAWSCredentials());
                    break;
            }
        }
    }
}
