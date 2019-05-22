using Amazon;
using Amazon.Runtime;
using Amazon.CloudFront;
using Amazon.CloudFront.Model;
using System;
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

        public async Task<string> CreateInvalidation(string distributionId)
        {
            var callerReference = Guid.NewGuid().ToString();
            var createInvalidationResponse = await amazonCloudFrontClient.CreateInvalidationAsync(new CreateInvalidationRequest()
            {
                DistributionId = distributionId,
                InvalidationBatch = new InvalidationBatch()
                {
                    Paths = new Paths() { Items = new List<string>() { "/*" } , Quantity = 1 },
                    CallerReference = callerReference
                }
            });
            return createInvalidationResponse.Invalidation.Id;
        }
    }
}
