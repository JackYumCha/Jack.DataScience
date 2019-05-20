using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.Batch;
using Amazon.Batch.Model;
using Amazon.Runtime;

namespace Jack.DataScience.Compute.AWSBatch
{
    public class AWSBatchAPI
    {
        private readonly AWSBatchOptions awsBatchOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonBatchClient amazonBatchClient;
        public AWSBatchAPI(AWSBatchOptions awsBatchOptions)
        {
            this.awsBatchOptions = awsBatchOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsBatchOptions.Key, awsBatchOptions.Secret);
            amazonBatchClient = new AmazonBatchClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsBatchOptions.Region));
        }

        public async Task<bool> SubmitJob(string name, string jobARN, string queueARN, Dictionary<string, string> parameters)
        {
            await amazonBatchClient.SubmitJobAsync(new SubmitJobRequest()
            {
                JobName = name,
                JobDefinition = jobARN,
                JobQueue = queueARN,
                Parameters = parameters
            });
            return true;   
        }
    }
}
