using System.Diagnostics;
using System.Threading;
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
            bool shouldTry = true;
            while(shouldTry)
            {
                var messageResource = await MessageResource.CreateAsync(new CreateMessageOptions(number)
                {
                    From = from,
                    Body = message
                });
                shouldTry = false;
                // the error code 429 is when concurrent limit is reached, then we should retry.
                if(messageResource.ErrorCode == 429)
                {
                    Thread.Sleep(1); // sleep 1ms to retry
                    shouldTry = true;
                }
            }
        }
    }
}
