using Jack.DataScience.Storage.AWSS3;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Jack.DataScience.Http.AWSCloudFront.Deploy
{
    public class CloudFrontDeployAPI
    {
        private readonly CloudFrontDeployOptions cloudFrontDeployOptions;
        private readonly AWSS3API awsS3API;
        private readonly AWSCloudFrontAPI awsCloudFrontAPI;
        public CloudFrontDeployAPI(CloudFrontDeployOptions cloudFrontDeployOptions)
        {
            if (cloudFrontDeployOptions.S3BasePath == null) cloudFrontDeployOptions.S3BasePath = "";
            this.cloudFrontDeployOptions = cloudFrontDeployOptions;
            awsS3API = new AWSS3API(cloudFrontDeployOptions.AWSS3Options);
            if(!string.IsNullOrWhiteSpace(cloudFrontDeployOptions.CloudFrontDistributionId))
            {
                awsCloudFrontAPI = new AWSCloudFrontAPI(cloudFrontDeployOptions.AWSCloudFrontOptions);
            }
        }

        public async Task<bool> Deploy()
        {
            if (!Directory.Exists(cloudFrontDeployOptions.ArtifactPath)) return false;
            DirectoryInfo artifactRoot = new DirectoryInfo(cloudFrontDeployOptions.ArtifactPath);

            // delete all files in S3 bucket
            var checkHTMLArtifactFile = cloudFrontDeployOptions.DefaultDeleteSafetyCheck;
            if (string.IsNullOrWhiteSpace(checkHTMLArtifactFile))
            {
                checkHTMLArtifactFile = "index.html";
            }

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Check for File: {cloudFrontDeployOptions.S3BasePath}{checkHTMLArtifactFile}");
            // avoid deleting the wrong bucket
            if (await awsS3API.FileExists($"{cloudFrontDeployOptions.S3BasePath}{checkHTMLArtifactFile}"))
            {
                var allObjects = await awsS3API.ListAllObjectsInBucket(prefix: cloudFrontDeployOptions.S3BasePath);
                Console.ForegroundColor = ConsoleColor.Yellow;
                foreach(var obj in allObjects)
                {
                    await awsS3API.Delete(obj.Key);
                    Console.WriteLine($"Deleted s3://{obj.BucketName}/{obj.Key}");
                }
            }

            // upload all files
            Console.ForegroundColor = ConsoleColor.Green;
            foreach (var file in ListAllFilesInDirectory(artifactRoot, cloudFrontDeployOptions.LocalFilePatterns
                .Select(value => new Regex(value))))
            {
                using (var fileStream = File.OpenRead($"{artifactRoot.FullName}/{file}"))
                {
                    if (cloudFrontDeployOptions.Private)
                    {
                        await awsS3API.Upload($"{cloudFrontDeployOptions.S3BasePath}{file}", fileStream);
                    }
                    else
                    {
                        await awsS3API.UploadAsPublic($"{cloudFrontDeployOptions.S3BasePath}{file}", fileStream);
                    }
                    
                    Console.WriteLine($"Uploaded {cloudFrontDeployOptions.S3BasePath}{file}");
                }
            }
            if (!string.IsNullOrWhiteSpace(cloudFrontDeployOptions.CloudFrontDistributionId))
            {
                var invalidationId = await awsCloudFrontAPI.CreateInvalidation(cloudFrontDeployOptions.CloudFrontDistributionId);
                Console.ForegroundColor = ConsoleColor.Blue;
                Console.WriteLine($"CloudFront Invalidation ID: {invalidationId}");
            }
            return true;
        }

        private IEnumerable<string> ListAllFilesInDirectory(DirectoryInfo directoryInfo, IEnumerable<Regex> patterns)
        {
            foreach (var file in directoryInfo.GetFiles())
            {
                if (!patterns.Any() || patterns.Any(rgx => rgx.IsMatch(file.Name)))
                    yield return file.Name;
            }

            foreach(var directory in directoryInfo.GetDirectories())
                foreach (var file in ListAllFilesInDirectory(directory, patterns))
                    yield return $"{directory.Name}/{file}";
        }
 
    }
}
