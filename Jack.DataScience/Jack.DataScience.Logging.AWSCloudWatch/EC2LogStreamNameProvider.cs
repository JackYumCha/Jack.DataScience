using Serilog.Sinks.AwsCloudWatch;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Logging.AWSCloudWatch
{
    public class EC2LogStreamNameProvider : ILogStreamNameProvider
    {
        private readonly DefaultLogStreamProvider defaultLogStreamProvider = new DefaultLogStreamProvider();
        public EC2LogStreamNameProvider()
        {
        }

        public string GetLogStreamName()
        {
            var ec2_id = Environment.GetEnvironmentVariable("EC2_ID");
            if (string.IsNullOrWhiteSpace(ec2_id)) return defaultLogStreamProvider.GetLogStreamName();
            return $"{DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss")}-EC2-{ec2_id}";
        }
    }
}
