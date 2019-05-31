using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Jack.DataScience.MQ.AWSSQS
{
    public class AWSSQSAPI
    {
        private readonly AWSSQSOptions awsSQSOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonSQSClient amazonSQSClient;
        public AWSSQSAPI(AWSSQSOptions awsSQSOptions)
        {
            this.awsSQSOptions = awsSQSOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsSQSOptions.Key, awsSQSOptions.Secret);
            amazonSQSClient = new AmazonSQSClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsSQSOptions.Region));
        }

        public async Task<HttpStatusCode> SendMessage(string message)
        {
            var sendMessageResponse = await amazonSQSClient.SendMessageAsync(new SendMessageRequest()
            {
                QueueUrl = awsSQSOptions.Url,
                MessageBody = message,
                DelaySeconds = 0,
                //MessageGroupId = groupId
            });
            return sendMessageResponse.HttpStatusCode;
        }

        public async Task<HttpStatusCode> DeleteMessage(string receiptHandle)
        {
            var sendMessageResponse = await amazonSQSClient.DeleteMessageAsync(awsSQSOptions.Url, receiptHandle);
            return sendMessageResponse.HttpStatusCode;
        }
    }
}
