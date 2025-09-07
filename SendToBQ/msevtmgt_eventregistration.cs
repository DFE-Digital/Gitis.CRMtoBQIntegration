using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class msevtmgt_eventregistration
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<msevtmgt_eventregistration> _logger;

        public msevtmgt_eventregistration(BQProcessor processor, ILogger<msevtmgt_eventregistration> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("msevtmgt_eventregistration")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("msevtmgt_eventregistration timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("msevtmgt_eventregistration");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed msevtmgt_eventregistration timer");
                throw;
            }
        }
    }
}