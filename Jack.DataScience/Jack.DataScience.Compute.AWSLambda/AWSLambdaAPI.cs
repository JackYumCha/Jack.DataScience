using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using Amazon;
using Amazon.Lambda;
using Amazon.Runtime;
using Amazon.Lambda.Model;
using Newtonsoft.Json;

namespace Jack.DataScience.Compute.AWSLambda
{
    public class AWSLambdaAPI
    {
        private readonly AWSLambdaOptions awsLambdaOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonLambdaClient amazonLambdaClient;
        public AWSLambdaAPI(AWSLambdaOptions awsLambdaOptions)
        {
            this.awsLambdaOptions = awsLambdaOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsLambdaOptions.Key, awsLambdaOptions.Secret);
            amazonLambdaClient = new AmazonLambdaClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsLambdaOptions.Region));
        }

        public async Task Invoke(string name, object parameter)
        {

            await amazonLambdaClient.InvokeAsync(new InvokeRequest()
            {
                FunctionName = name,
                Payload = JsonConvert.SerializeObject(parameter),
                InvocationType = InvocationType.Event
            });
        }
    }
}
