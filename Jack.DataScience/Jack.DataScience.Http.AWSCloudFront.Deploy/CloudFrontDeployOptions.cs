using Jack.DataScience.Storage.AWSS3;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Http.AWSCloudFront.Deploy
{
    public class CloudFrontDeployOptions
    {
        public AWSCloudFrontOptions AWSCloudFrontOptions { get; set; }
        public AWSS3Options AWSS3Options { get; set; }
        public string CloudFrontDistributionId { get; set; }
        public string ArtifactPath { get; set; }
        public string DefaultDeleteSafetyCheck { get; set; }
        public string S3BasePath { get; set; }
        public bool Private { get; set; }
        public List<string> LocalFilePatterns { get; set; }
    }
}
