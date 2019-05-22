using Jack.DataScience.Console;
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

            System.Console.ForegroundColor = ConsoleColor.Green;
            System.Console.Write($"Artifact: ");
            System.Console.ForegroundColor = ConsoleColor.Yellow;
            System.Console.WriteLine(cloudFrontDeployOptions.ArtifactPath);
            System.Console.ForegroundColor = ConsoleColor.Green;
            System.Console.Write($"Distribution ID: ");
            System.Console.ForegroundColor = ConsoleColor.Yellow;
            System.Console.WriteLine(cloudFrontDeployOptions.CloudFrontDistributionId);

            System.Console.ForegroundColor = ConsoleColor.White;

            CloudFrontDeployAPI cloudFrontDeployAPI = new CloudFrontDeployAPI(cloudFrontDeployOptions);

            var result = cloudFrontDeployAPI.Deploy().GetAwaiter().GetResult();

            System.Console.ForegroundColor = ConsoleColor.White;
        }
    }
}
