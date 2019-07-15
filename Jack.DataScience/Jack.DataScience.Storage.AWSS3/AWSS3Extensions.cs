using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Jack.DataScience.Storage.AWSS3
{
    public static class AWSS3Extensions
    {
        private static Regex S3URIRegex = new Regex(@"^s3:\/\/([a-z0-9.-]{3,63})\/([\w\W]+)$");

        public static S3Object ParseS3URI(this string uri)
        {
            var match = S3URIRegex.Match(uri);
            if (!match.Success) return null;
            return new S3Object()
            {
                BucketName = match.Groups[1].Value,
                Key = match.Groups[2].Value
            };
        }

    }
}
