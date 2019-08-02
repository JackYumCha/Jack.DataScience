using Jack.DataScience.ConsoleExtensions;
using Jack.DataScience.Storage.AWSS3;
using System;
using System.IO;

namespace Jack.DataScience.Http.AWSCloudFront.Deploy.CLI
{
    class Program
    {
        static void Main(string[] args)
        {
            AWSS3Options awsS3Options = new AWSS3Options();
            AWSCloudFrontOptions awsCloudFrontOptions = new AWSCloudFrontOptions();

            awsS3Options.Key = args.GetParameter("--s3-key");
            awsS3Options.Secret = args.GetParameter("--s3-secret");
            awsS3Options.Region = args.GetParameter("--s3-region");
            awsS3Options.Bucket = args.GetParameter("--s3-bucket");

            awsCloudFrontOptions.Key = args.GetParameter("--cloudfront-key");
            awsCloudFrontOptions.Secret = args.GetParameter("--cloudfront-secret");
            awsCloudFrontOptions.Region = args.GetParameter("--cloudfront-region");

            CloudFrontDeployOptions cloudFrontDeployOptions = new CloudFrontDeployOptions();

            cloudFrontDeployOptions.AWSS3Options = awsS3Options;
            cloudFrontDeployOptions.AWSCloudFrontOptions = awsCloudFrontOptions;

            var path = args.GetParameter("--artifact-path");

            cloudFrontDeployOptions.ArtifactPath = $"{Directory.GetCurrentDirectory()}/{path}";
            cloudFrontDeployOptions.CloudFrontDistributionId = args.GetParameter("--cloudfront-distribution-id");
            cloudFrontDeployOptions.DefaultDeleteSafetyCheck = args.GetParameter("--s3-delete-check");
            cloudFrontDeployOptions.Private = args.HasParameter("--private");
            cloudFrontDeployOptions.S3BasePath = args.GetParameter("--s3-base-path");

            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write($"Artifact: ");
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(cloudFrontDeployOptions.ArtifactPath);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write($"Distribution ID: ");
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(cloudFrontDeployOptions.CloudFrontDistributionId);

            Console.ForegroundColor = ConsoleColor.White;

            CloudFrontDeployAPI cloudFrontDeployAPI = new CloudFrontDeployAPI(cloudFrontDeployOptions);

            var result = cloudFrontDeployAPI.Deploy().GetAwaiter().GetResult();

            Console.ForegroundColor = ConsoleColor.White;
        }
    }
}
