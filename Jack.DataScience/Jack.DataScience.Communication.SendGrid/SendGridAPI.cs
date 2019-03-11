using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using SendGrid;
using SendGrid.Helpers.Mail;

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

        public async Task SendEmail(string to, string from, string subject, string body)
        {
            var message = MailHelper.CreateSingleEmail(new EmailAddress(from),
                new EmailAddress(to),
                subject, null, body);
            await sendGridClient.SendEmailAsync(message);
        }
    }
}
