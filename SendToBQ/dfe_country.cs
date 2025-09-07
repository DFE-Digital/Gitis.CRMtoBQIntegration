using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_country
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_country> _logger;

        public dfe_country(BQProcessor processor, ILogger<dfe_country> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_country")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_country timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_country");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_country timer");
                throw;
            }
        }
    }
}