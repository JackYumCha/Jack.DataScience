using System;
using System.Collections.Generic;
using System.Linq;
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

        public async Task<List<string>> ListJobs(string queueARN)
        {
            var response = await amazonBatchClient.ListJobsAsync(new ListJobsRequest()
            {
                JobQueue = queueARN
            });

            return response.JobSummaryList.Select(j => j.JobId).ToList();
        }

        public async Task<bool> CancelJobs(IEnumerable<string> jobIDs, string reason = "cancel")
        {
            foreach(var jobID in jobIDs)
            {
                await amazonBatchClient.CancelJobAsync(new CancelJobRequest()
                {
                    JobId = jobID,
                    Reason = reason
                });
            }
            return true;
        }

        public async Task<bool> CancelJob(string jobID, string reason = "cancel")
        {
            await amazonBatchClient.CancelJobAsync(new CancelJobRequest()
            {
                JobId = jobID,
                Reason = reason
            });
            return true;
        }
    }
}
