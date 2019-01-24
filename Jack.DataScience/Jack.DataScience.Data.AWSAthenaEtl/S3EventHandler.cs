using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    /// <summary>
    /// this keeps the s3 etl process method
    /// </summary>
    public class S3EventHandler
    {
        public string BucketName { get; set; }
        public string PathRegex { get; set; }
        public string EtlName { get; set; }
    }
}
