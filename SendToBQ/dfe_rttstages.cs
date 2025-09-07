using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_rttstages
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_rttstages> _logger;

        public dfe_rttstages(BQProcessor processor, ILogger<dfe_rttstages> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_rttstages")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_rttstages timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_rttstages");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_rttstages timer");
                throw;
            }
        }
    }
}