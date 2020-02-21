using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.EC2;
using Amazon.EC2.Model;

namespace Jack.DataScience.Compute.AWSEC2
{
    public class AWSEC2API
    {
        private readonly AWSEC2Options awsEC2Options;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonEC2Client amazonEC2Client;
        private readonly SessionAWSCredentials sessionAWSCredentials;
        public AWSEC2API(AWSEC2Options awsEC2Options)
        {
            this.awsEC2Options = awsEC2Options;
            basicAWSCredentials = new BasicAWSCredentials(awsEC2Options.Key, awsEC2Options.Secret);
            amazonEC2Client = new AmazonEC2Client(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsEC2Options.Region));
        }
        public AWSEC2API(SessionAWSCredentials sessionAWSCredentials)
        {
            this.sessionAWSCredentials = sessionAWSCredentials;
            var credentials = sessionAWSCredentials.GetCredentials();
            basicAWSCredentials = new BasicAWSCredentials(credentials.AccessKey, credentials.SecretKey);
            amazonEC2Client = new AmazonEC2Client(sessionAWSCredentials);
        }

        public AWSEC2Options AWSEC2Options { get => awsEC2Options; }

        /// <summary>
        /// Execute the defined job in the EC2
        /// </summary>
        /// <returns></returns>
        public async Task ExecuteJob()
        {
            if(awsEC2Options.StartIds is List<string>)
            {
                await amazonEC2Client.StartInstancesAsync(new StartInstancesRequest()
                {
                    InstanceIds = awsEC2Options.StartIds
                });
            }

            if(awsEC2Options.StopIds is List<string>)
            {
                await amazonEC2Client.StopInstancesAsync(new StopInstancesRequest()
                {
                    InstanceIds = awsEC2Options.StopIds,
                    Force = true,
                    Hibernate = false
                });
            }
        }

        public async Task RebootByIds(List<string> instanceIds)
        {
            if (instanceIds is List<string>)
            {
                await amazonEC2Client.RebootInstancesAsync(new RebootInstancesRequest()
                {
                    InstanceIds = instanceIds
                });
            }
        }

        public async Task StartByIds(List<string> instanceIds)
        {
            if (instanceIds is List<string>)
            {
                await amazonEC2Client.StartInstancesAsync(new StartInstancesRequest()
                {
                    InstanceIds = instanceIds
                });
            }
        }

        public async Task StopByIds(List<string> instanceIds)
        {
            if (instanceIds is List<string>)
            {
                await amazonEC2Client.StopInstancesAsync(new StopInstancesRequest()
                {
                    InstanceIds = instanceIds,
                    Force = true,
                    Hibernate = false
                });
            }
        }

        public async Task TerminateByIds(List<string> instanceIds)
        {
            if (instanceIds is List<string>)
            {
                await amazonEC2Client.TerminateInstancesAsync(new TerminateInstancesRequest()
                {
                    InstanceIds = instanceIds
                });
            }
        }

        public async Task<RunInstancesResponse> RunInstanceByTemplate(string launchTemplateId, int count = 1)
        {
            return await amazonEC2Client.RunInstancesAsync(new RunInstancesRequest()
            {
                LaunchTemplate = new LaunchTemplateSpecification()
                {
                    LaunchTemplateId = launchTemplateId
                },
                MinCount = count,
                MaxCount = count,
            });
        }

        public async Task<List<Reservation>> DescribeAllInstances()
        {
            var results = new List<Reservation>();
            string token = "";
            do
            {
                var response = await amazonEC2Client.DescribeInstancesAsync(new DescribeInstancesRequest()
                {
                    NextToken = token
                });
                results.AddRange(response.Reservations);
                token = response.NextToken;
            } while (token != null);
            return results;
        }
    }
}
