using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class msevtmgt_event
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<msevtmgt_event> _logger;

        public msevtmgt_event(BQProcessor processor, ILogger<msevtmgt_event> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("msevtmgt_event")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("msevtmgt_event timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("msevtmgt_event");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed msevtmgt_event timer");
                throw;
            }
        }
    }
}