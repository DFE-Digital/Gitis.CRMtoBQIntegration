
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_applyreference
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_applyreference> _logger;

        public dfe_applyreference(BQProcessor processor, ILogger<dfe_applyreference> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_applyreference")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_applyreference timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_applyreference");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_applyreference timer");
                throw;
            }
        }
    }
}

