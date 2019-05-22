using Amazon.Runtime;
using Amazon.CloudWatchEvents;
using Amazon.CloudWatchEvents.Model;
using System;
using Amazon;
using System.Threading.Tasks;
using System.Net;

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

        public async Task<HttpStatusCode> DisableRule(string name)
        {
            var disableRuleResponse = await amazonCloudWatchEventsClient.DisableRuleAsync(new DisableRuleRequest()
            {
                Name = name
            });
            return disableRuleResponse.HttpStatusCode;
        }

        public async Task<HttpStatusCode> EnableRule(string name)
        {
            var enableRuleResponse = await amazonCloudWatchEventsClient.EnableRuleAsync(new EnableRuleRequest()
            {
                Name = name
            });
            return enableRuleResponse.HttpStatusCode;
        }

        public async Task<string> GetRuleState(string name)
        {
            var describeRuleResponse = await amazonCloudWatchEventsClient.DescribeRuleAsync(new DescribeRuleRequest()
            {
                Name = name
            });
            return describeRuleResponse.State.Value;
        }
    }
}
