using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using SendGrid;
using SendGrid.Helpers.Mail;
using System.Net;

namespace Jack.DataScience.Communication.SendGrid
{
    public class SendGridAPI
    {
        private readonly SendGridClient sendGridClient;

        public SendGridAPI(SendGridOptions sendGridOptions)
        {
            SendGridClientOptions sendGridClientOptions = new SendGridClientOptions()
            {
                ApiKey = sendGridOptions.Key
            };
            sendGridClient = new SendGridClient(sendGridClientOptions);
        }

        public async Task<HttpStatusCode> SendEmail(string to, string from, string subject, string body)
        {
            var message = MailHelper.CreateSingleEmail(new EmailAddress(from),
                new EmailAddress(to),
                subject, null, body);
            var response =  await sendGridClient.SendEmailAsync(message);
            return response.StatusCode;
        }
    }
}
