using Jack.DataScience.Compute.AWSEC2;
using Jack.DataScience.Data.AWSDynamoDB;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Cloud.HeartBeat
{
    public class HeartBeatOptions
    {
        public AWSEC2Options EC2 { get; set; }
        public AWSDynamoDBOptions DynamoDB { get; set; }
        public string DefaultLaunchTemplateId { get; set; }
    }
}
