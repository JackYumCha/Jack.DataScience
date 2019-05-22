using Amazon;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using System;

namespace Jack.DataScience.AWS.SecurityToken
{
    public class AWSSecurityTokenAPI
    {
        private readonly AmazonSecurityTokenServiceClient amazonSecurityTokenServiceClient;

        public async void Test()
        {
            var result = await amazonSecurityTokenServiceClient.AssumeRoleAsync(new AssumeRoleRequest()
            {
                DurationSeconds = 900,
                RoleArn = "arn:aws:iam::aws:policy/AdministratorAccess",
                
            });
            
        }
    }
}
