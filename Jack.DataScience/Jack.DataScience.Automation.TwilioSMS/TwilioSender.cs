using System.Diagnostics;
using System.Threading.Tasks;
using Twilio;
using Twilio.Rest.Api.V2010.Account;

namespace Jack.DataScience.Automation.TwilioSMS
{

    public class TwilioSender
    {
        private readonly TwilioOptions options;

        public TwilioSender(TwilioOptions options)
        {
            TwilioClient.Init(options.AccoundSID, options.Token);
            this.options = options;
        }

        public async Task<long> MeasureSend(string number, string message, string from)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var messageResource = await MessageResource.CreateAsync(new CreateMessageOptions(number)
            {
                From = from,
                Body = message
            });
            stopwatch.Stop();
            return stopwatch.ElapsedMilliseconds;
        }

        public async Task Send(string number, string message, string from)
        {
            var messageResource = await MessageResource.CreateAsync(new CreateMessageOptions(number)
            {
                From = from,
                Body = message
            });
        }
    }
}
