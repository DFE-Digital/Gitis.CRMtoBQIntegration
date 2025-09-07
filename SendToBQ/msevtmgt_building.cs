using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class msevtmgt_building
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<msevtmgt_building> _logger;

        public msevtmgt_building(BQProcessor processor, ILogger<msevtmgt_building> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("msevtmgt_building")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("msevtmgt_building timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("msevtmgt_building");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed msevtmgt_building timer");
                throw;
            }
        }
    }
}