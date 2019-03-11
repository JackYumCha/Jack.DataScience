using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;

namespace Jack.DataScience.Communication.AWSSimpleEmail
{
    public class AWSSimpleEmailAPI
    {
        private readonly SimpleEmailOptions simpleEmailOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonSimpleEmailServiceClient amazonSimpleEmailServiceClient;

        public AWSSimpleEmailAPI(SimpleEmailOptions simpleEmailOptions)
        {
            this.simpleEmailOptions = simpleEmailOptions;
            basicAWSCredentials = new BasicAWSCredentials(simpleEmailOptions.Key, simpleEmailOptions.Secret);
            amazonSimpleEmailServiceClient = new AmazonSimpleEmailServiceClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(simpleEmailOptions.Region));
        }

        public async Task SendEmail(string to, string from, string subject, string body)
        {
            await amazonSimpleEmailServiceClient.SendEmailAsync(new SendEmailRequest()
            {
                Destination = new Destination(new List<string>() { to }),
                Source = from,
                ReplyToAddresses = new List<string>() { from },
                Message = new Message(new Content(subject), new Body(new Content(body)))
            });
        }
    }
}
