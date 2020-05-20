using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using Amazon;
using Amazon.Lambda;
using Amazon.Runtime;
using Amazon.Lambda.Model;
using Newtonsoft.Json;
using SystemEnvironment = System.Environment;
using System.Threading;

namespace Jack.DataScience.Compute.AWSLambda
{
    public class AWSLambdaAPI
    {
        private readonly AWSLambdaOptions awsLambdaOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonLambdaClient amazonLambdaClient;
        private readonly SessionAWSCredentials sessionAWSCredentials;

        public AWSLambdaAPI(AWSLambdaOptions awsLambdaOptions)
        {
            this.awsLambdaOptions = awsLambdaOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsLambdaOptions.Key, awsLambdaOptions.Secret);
            amazonLambdaClient = new AmazonLambdaClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsLambdaOptions.Region));
        }

        public AWSLambdaAPI(SessionAWSCredentials sessionAWSCredentials)
        {
            this.sessionAWSCredentials = sessionAWSCredentials;
            var credentials = sessionAWSCredentials.GetCredentials();
            basicAWSCredentials = new BasicAWSCredentials(credentials.AccessKey, credentials.SecretKey);
            amazonLambdaClient = new AmazonLambdaClient(sessionAWSCredentials);
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

        public async Task InvokeSelfAndWait(object parameter, int waitTime)
        {
            string AWS_LAMBDA_FUNCTION_NAME = SystemEnvironment.GetEnvironmentVariable(nameof(AWS_LAMBDA_FUNCTION_NAME));
            await amazonLambdaClient.InvokeAsync(new InvokeRequest()
            {
                FunctionName = AWS_LAMBDA_FUNCTION_NAME,
                Payload = JsonConvert.SerializeObject(parameter),
                InvocationType = InvocationType.Event
            });
            Thread.Sleep(waitTime);
        }
    }
}
