using System;
using System.Collections.Generic;
using System.Text;
using MvcAngular;


namespace Jack.DataScience.Data.AWSAthenaEtl
{
    [AngularType]
    public enum EtlSourceEnum
    {
        SFTP,
        S3BucketCheck,
        S3BucketEvent,
        GoogleAnalytics,
        AmazonAthena
    }

    [AngularType]
    public enum EtlFileType
    {
        CSV,
        Parquet,
        XML
    }
}
