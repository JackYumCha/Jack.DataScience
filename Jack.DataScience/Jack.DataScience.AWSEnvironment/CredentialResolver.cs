using Amazon;
using Amazon.Runtime;
using System;

namespace Jack.DataScience.AWSEnvironment
{
    public static class CredentialResolver
    {
        public static AWSEnvironmentCredential ResolveFromAWSEnvironemt()
        {
            AWSEnvironmentCredential credential = new AWSEnvironmentCredential();
 
            credential.Access = Environment.GetEnvironmentVariable(nameof(AWSEnvironmentCredential.Access));
            credential.Secret = Environment.GetEnvironmentVariable(nameof(AWSEnvironmentCredential.Secret));
            credential.Region = Environment.GetEnvironmentVariable(nameof(AWSEnvironmentCredential.Region));

            credential.Region = string.IsNullOrEmpty(credential.Region) ? Environment.GetEnvironmentVariable("AWS_REGION") : credential.Region;
            if (string.IsNullOrEmpty(credential.Access) || string.IsNullOrEmpty(credential.Secret))
            {
                credential.Access = string.IsNullOrEmpty(credential.Access) ? Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID") : credential.Access;
                credential.Secret = string.IsNullOrEmpty(credential.Secret) ? Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY") : credential.Secret;
                credential.Token = Environment.GetEnvironmentVariable("AWS_SESSION_TOKEN");
                credential.Mode = CredentialModeEnum.AccessSecretToken;
            }
            else
            {
                credential.Mode = CredentialModeEnum.AccessSecretRegion;
            }
            return credential;
        }

        public static SessionAWSCredentials CreateSessionAWSCredentials(this AWSEnvironmentCredential credential)
        {
            return new SessionAWSCredentials(credential.Access, credential.Secret, credential.Token);
        }

        public static BasicAWSCredentials CreateBasicAWSCredentials(this AWSEnvironmentCredential credential)
        {
            return new BasicAWSCredentials(credential.Access, credential.Secret);
        }

        public static RegionEndpoint CreateRegionEndpoint(this AWSEnvironmentCredential credential)
        {
            return RegionEndpoint.GetBySystemName(credential.Region);
        }
    }
}
