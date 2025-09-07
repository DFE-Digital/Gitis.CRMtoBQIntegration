using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class msevtmgt_checkin
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<msevtmgt_checkin> _logger;

        public msevtmgt_checkin(BQProcessor processor, ILogger<msevtmgt_checkin> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("msevtmgt_checkin")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("msevtmgt_checkin timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("msevtmgt_checkin");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed msevtmgt_checkin timer");
                throw;
            }
        }
    }
}