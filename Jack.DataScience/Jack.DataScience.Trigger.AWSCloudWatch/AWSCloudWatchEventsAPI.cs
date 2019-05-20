using Amazon.Runtime;
using Amazon.CloudWatchEvents;
using Amazon.CloudWatchEvents.Model;
using System;
using Amazon;
using System.Threading.Tasks;

namespace Jack.DataScience.Trigger.AWSCloudWatch
{
    public class AWSCloudWatchEventsAPI
    {
        private readonly AWSCloudWatchEventsOptions awsCloudWatchEventOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonCloudWatchEventsClient amazonCloudWatchEventsClient;
        public AWSCloudWatchEventsAPI(AWSCloudWatchEventsOptions awsCloudWatchEventOptions)
        {
            this.awsCloudWatchEventOptions = awsCloudWatchEventOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsCloudWatchEventOptions.Key, awsCloudWatchEventOptions.Secret);
            amazonCloudWatchEventsClient = new AmazonCloudWatchEventsClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsCloudWatchEventOptions.Region));
        }

        public async Task DisableRule(string name)
        {
            await amazonCloudWatchEventsClient.DisableRuleAsync(new DisableRuleRequest()
            {
                Name = name
            });
        }

        public async Task EnableRule(string name)
        {
            await amazonCloudWatchEventsClient.EnableRuleAsync(new EnableRuleRequest()
            {
                Name = name
            });
        }
    }
}
