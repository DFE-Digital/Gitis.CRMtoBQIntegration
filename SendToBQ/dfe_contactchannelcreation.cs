using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_contactchannelcreation
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_contactchannelcreation> _logger;

        public dfe_contactchannelcreation(BQProcessor processor, ILogger<dfe_contactchannelcreation> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_contactchannelcreation")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_contactchannelcreation timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_contactchannelcreation");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_contactchannelcreation timer");
                throw;
            }
        }
    }
}