using Amazon;
using Amazon.Runtime;
using Amazon.CloudFront;
using Amazon.CloudFront.Model;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Jack.DataScience.Http.AWSCloudFront
{
    public class AWSCloudFrontAPI
    {
        private readonly AWSCloudFrontOptions awsCloudFrontOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonCloudFrontClient amazonCloudFrontClient;

        public AWSCloudFrontAPI(AWSCloudFrontOptions awsCloudFrontOptions)
        {
            this.awsCloudFrontOptions = awsCloudFrontOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsCloudFrontOptions.Key, awsCloudFrontOptions.Secret);
            amazonCloudFrontClient = new AmazonCloudFrontClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsCloudFrontOptions.Region));
        }

        public async Task CreateInvalidation(string distributionId)
        {
            await amazonCloudFrontClient.CreateInvalidationAsync(new CreateInvalidationRequest()
            {
                DistributionId = distributionId,
                InvalidationBatch = new InvalidationBatch()
                {
                    Paths = new Paths() { Items = new List<string>() { "*" } }
                }
            });
        }
    }
}
