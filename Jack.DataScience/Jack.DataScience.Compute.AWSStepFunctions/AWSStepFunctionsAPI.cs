using Amazon;
using Amazon.Runtime;
using Amazon.StepFunctions;
using Amazon.StepFunctions.Model;
using System;
using System.Threading.Tasks;
using System.Linq;
using System.Net;

namespace Jack.DataScience.Compute.AWSStepFunctions
{
    public class AWSStepFunctionsAPI
    {
        private readonly AWSStepFunctionsOptions awsStepFunctionsOptions;
        private readonly SessionAWSCredentials sessionAWSCredentials;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonStepFunctionsClient amazonStepFunctionsClient;

        public AWSStepFunctionsAPI(AWSStepFunctionsOptions awsStepFunctionsOptions)
        {
            this.awsStepFunctionsOptions = awsStepFunctionsOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsStepFunctionsOptions.Key, awsStepFunctionsOptions.Secret);
            amazonStepFunctionsClient = new AmazonStepFunctionsClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsStepFunctionsOptions.Region));
        }

        public AWSStepFunctionsAPI(SessionAWSCredentials sessionAWSCredentials)
        {
            this.sessionAWSCredentials = sessionAWSCredentials;
            var credentials = sessionAWSCredentials.GetCredentials();
            basicAWSCredentials = new BasicAWSCredentials(credentials.AccessKey, credentials.SecretKey);
            amazonStepFunctionsClient = new AmazonStepFunctionsClient(sessionAWSCredentials);
        }

        public async Task<string> StartFunction(string stateMachineArn, string input, string name = null)
        {
            var response = await amazonStepFunctionsClient.StartExecutionAsync(new StartExecutionRequest()
            {
                StateMachineArn =stateMachineArn,
                Input = input,
                Name = name
            });
            switch (response.HttpStatusCode)
            {
                case HttpStatusCode.Accepted:
                case HttpStatusCode.OK:
                    return response.ExecutionArn;
                default:
                    return null;
            }
        }

        public async Task StopFunction(string stateMachineArn, string error, string cause = null)
        {
            await amazonStepFunctionsClient.StopExecutionAsync(new StopExecutionRequest()
            {
                ExecutionArn = stateMachineArn,
                Error = error,
                Cause = cause
            });
        }
    }
}
