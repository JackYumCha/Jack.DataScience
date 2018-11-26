using System;
using System.Collections.Generic;
using System.Text;
using Amazon.Athena;
using Amazon.Athena.Model;

namespace Jack.DataScience.Data.AWSAthena
{
    public class Athena
    {
        private readonly AmazonAthenaClient amazonAthenaClient;
        public Athena(AmazonAthenaClient amazonAthenaClient)
        {
            this.amazonAthenaClient = amazonAthenaClient;
        }

        public void Test()
        {
            amazonAthenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest()
            {
                QueryString = "",
                QueryExecutionContext = new QueryExecutionContext()
                {
                },
                ClientRequestToken = "",
                ResultConfiguration = new ResultConfiguration
                {
                    EncryptionConfiguration = new EncryptionConfiguration
                    {
                        EncryptionOption = EncryptionOption.SSE_S3
                    },
                    OutputLocation = "s3;//abcd.ef"
                }
            });

            amazonAthenaClient.CreateNamedQueryAsync
        }
    }
}
