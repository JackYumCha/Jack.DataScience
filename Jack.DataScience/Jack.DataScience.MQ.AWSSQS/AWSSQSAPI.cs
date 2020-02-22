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
        private readonly SessionAWSCredentials sessionAWSCredentials;
        public AWSSQSAPI(AWSSQSOptions awsSQSOptions)
        {
            this.awsSQSOptions = awsSQSOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsSQSOptions.Key, awsSQSOptions.Secret);
            amazonSQSClient = new AmazonSQSClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsSQSOptions.Region));
        }

        public AWSSQSAPI(SessionAWSCredentials sessionAWSCredentials)
        {
            this.sessionAWSCredentials = sessionAWSCredentials;
            var credentials = sessionAWSCredentials.GetCredentials();
            basicAWSCredentials = new BasicAWSCredentials(credentials.AccessKey, credentials.SecretKey);
            amazonSQSClient = new AmazonSQSClient(sessionAWSCredentials);
        }

        public async Task<HttpStatusCode> SendMessage(string message, string url = null)
        {
            if (url == null) url = awsSQSOptions.Url;
            var sendMessageResponse = await amazonSQSClient.SendMessageAsync(new SendMessageRequest()
            {
                QueueUrl = url,
                MessageBody = message,
                DelaySeconds = 0,
                //MessageGroupId = groupId
            });
            return sendMessageResponse.HttpStatusCode;
        }

        public async Task<HttpStatusCode> DeleteMessage(string receiptHandle, string url = null)
        {
            if (url == null) url = awsSQSOptions.Url;
            var sendMessageResponse = await amazonSQSClient.DeleteMessageAsync(url, receiptHandle);
            return sendMessageResponse.HttpStatusCode;
        }

        public async Task<List<Message>> ReceiveMessage(int maxNumberOfMessages, int visibilityTimeout, string url = null)
        {
            if (url == null) url = awsSQSOptions.Url;
            var receiveMessageResponse = await amazonSQSClient.ReceiveMessageAsync(new ReceiveMessageRequest()
            {
                QueueUrl = url,
                MaxNumberOfMessages = maxNumberOfMessages,
                VisibilityTimeout = visibilityTimeout,
            });
            return receiveMessageResponse.Messages;
        }

        public async Task<HttpStatusCode> Purge(string url = null)
        {
            if (url == null) url = awsSQSOptions.Url;
            try
            {
                var purgeQueueResponse = await amazonSQSClient.PurgeQueueAsync(url);
                return purgeQueueResponse.HttpStatusCode;
            }
            catch(PurgeQueueInProgressException ex)
            {
                return HttpStatusCode.Conflict;
            }
        }

        public async Task<int> CountMessages(string url = null)
        {
            if (url == null) url = awsSQSOptions.Url;
            var getQueueAttributesResponse = await amazonSQSClient.GetQueueAttributesAsync(new GetQueueAttributesRequest()
            {
                QueueUrl = url,
                AttributeNames = { "ApproximateNumberOfMessages" }
            });
            return getQueueAttributesResponse.ApproximateNumberOfMessages;
        }
    }
}
