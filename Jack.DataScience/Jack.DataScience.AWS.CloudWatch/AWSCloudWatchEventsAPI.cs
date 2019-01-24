using System;
using System.Collections.Generic;
using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.CloudWatchEvents;
using Amazon.CloudWatchEvents.Model;

namespace Jack.DataScience.AWS.CloudWatch
{
    public class AWSCloudWatchEventsAPI
    {
        private readonly AWSCloudWatchEventsOptions awsCloudWatchEventsOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonCloudWatchEventsClient amazonCloudWatchEventsClient;

        public AWSCloudWatchEventsAPI(AWSCloudWatchEventsOptions awsCloudWatchEventsOptions)
        {
            this.awsCloudWatchEventsOptions = awsCloudWatchEventsOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsCloudWatchEventsOptions.Key, awsCloudWatchEventsOptions.Secret);
            amazonCloudWatchEventsClient = new AmazonCloudWatchEventsClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsCloudWatchEventsOptions.Region));
        }

        public void SetupDailyEvent(string name, string cron)
        {
            amazonCloudWatchEventsClient.PutRuleAsync(new PutRuleRequest()
            {
                Name = name,
            });
            amazonCloudWatchEventsClient.EnableRuleAsync(new EnableRuleRequest()
            {
                Name = name,
            });
        }

    }
}
