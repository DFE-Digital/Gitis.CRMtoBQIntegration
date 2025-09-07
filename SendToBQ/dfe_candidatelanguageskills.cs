using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_candidatelanguageskills
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_candidatelanguageskills> _logger;

        public dfe_candidatelanguageskills(BQProcessor processor, ILogger<dfe_candidatelanguageskills> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_candidatelanguageskills")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_candidatelanguageskills timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_candidatelanguageskills");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_candidatelanguageskills timer");
                throw;
            }
        }
    }
}