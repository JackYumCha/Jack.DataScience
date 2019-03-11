using System;
using System.Collections.Generic;
using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

namespace Jack.DataScience.Communication.AWSSNS
{
    public class AWSSNSAPI
    {
        private readonly SNSOptions snsOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonSimpleNotificationServiceClient amazonSimpleNotificationServiceClient;
        public AWSSNSAPI(SNSOptions snsOptions)
        {
            this.snsOptions = snsOptions;
            basicAWSCredentials = new BasicAWSCredentials(snsOptions.Key, snsOptions.Secret);
            amazonSimpleNotificationServiceClient = new AmazonSimpleNotificationServiceClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(snsOptions.Region));
        }

    }
}
