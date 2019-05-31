using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Jack.DataScience.MQ.AWSKenesis
{
    public class AWSKinesisAPI
    {
        private readonly AWSKinesisOptions awsKinesisOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly  AmazonKinesisClient amazonKinesisClient;
        public AWSKinesisAPI(AWSKinesisOptions awsKinesisOptions)
        {
            this.awsKinesisOptions = awsKinesisOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsKinesisOptions.Key, awsKinesisOptions.Secret);
            amazonKinesisClient = new AmazonKinesisClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsKinesisOptions.Region));
        }

        public async Task<HttpStatusCode> PutRecord(MemoryStream stream, string partitionKey, string streamName = null)
        {
            if (streamName == null) streamName = awsKinesisOptions.Stream;
            var putRecordResponse = await amazonKinesisClient.PutRecordAsync(new PutRecordRequest()
            {
                StreamName = streamName,
                PartitionKey = partitionKey,
                Data = stream
            });
            return putRecordResponse.HttpStatusCode;
        }

        public async Task<HttpStatusCode> PutRecords(IEnumerable<byte[]> list , string partitionKey, string streamName = null)
        {
            if (streamName == null) streamName = awsKinesisOptions.Stream;
            var records = list.Select(data => new PutRecordsRequestEntry()
            {
                Data = new MemoryStream(data),
                PartitionKey = partitionKey,
            }).ToList();
            var putRecordResponse = await amazonKinesisClient.PutRecordsAsync(new PutRecordsRequest()
            {
                StreamName = streamName,
                Records = records
            });
            records.ForEach(record => record.Data.Dispose());
            return putRecordResponse.HttpStatusCode;
        }
    }
}
