using Amazon;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Jack.DataScience.Logging.AWSCloudWatch
{
    public class AWSCloudWatchAPI
    {
        private readonly AWSCloudWatchOptions awsCloudWatchOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonCloudWatchLogsClient amazonCloudWatchLogsClient;

        public AWSCloudWatchAPI(AWSCloudWatchOptions awsCloudWatchOptions)
        {
            this.awsCloudWatchOptions = awsCloudWatchOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsCloudWatchOptions.Key, awsCloudWatchOptions.Secret);
            amazonCloudWatchLogsClient = new AmazonCloudWatchLogsClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsCloudWatchOptions.Region));
        }

        static DateTime DateOrigin = new DateTime(1970, 1, 1, 0, 0, 0);

        public async Task CreateLogExport(string logGroupName, string destinationS3Bucket, DateTime from, DateTime to, string destinationPrefix, string logStreamPrefix,
            string taskName = null)
        {
            if (taskName == null) taskName = Guid.NewGuid().ToString();
            await amazonCloudWatchLogsClient.CreateExportTaskAsync(new CreateExportTaskRequest()
            {
                Destination = destinationS3Bucket,
                From = (long)Math.Round((from - DateOrigin).TotalMilliseconds),
                To = (long)Math.Round((to - DateOrigin).TotalMilliseconds),
                LogGroupName = logGroupName,
                DestinationPrefix = destinationPrefix,
                LogStreamNamePrefix = string.IsNullOrEmpty(logStreamPrefix)?null:logStreamPrefix,
                TaskName = taskName
            });
        }

        public async Task CreateLogExportAndWait(string logGroupName, string destinationS3Bucket, DateTime from, DateTime to, string destinationPrefix, string logStreamPrefix,
            string taskName = null, int waitTime = 1000, int times = 10)
        {
            if (taskName == null) taskName = Guid.NewGuid().ToString();

            bool exported = false;
            while (times > 0 && !exported)
            {
                try
                {
                    await amazonCloudWatchLogsClient.CreateExportTaskAsync(new CreateExportTaskRequest()
                    {
                        Destination = destinationS3Bucket,
                        From = (long)Math.Round((from - DateOrigin).TotalMilliseconds),
                        To = (long)Math.Round((to - DateOrigin).TotalMilliseconds),
                        LogGroupName = logGroupName,
                        DestinationPrefix = destinationPrefix,
                        LogStreamNamePrefix = string.IsNullOrEmpty(logStreamPrefix) ? null : logStreamPrefix,
                        TaskName = taskName
                    });
                    exported = true;
                }
                catch (LimitExceededException ex)
                {
                    // one export task at one time
                }
                Thread.Sleep(waitTime);
                times--;
            }


        }
    }
}
