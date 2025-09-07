using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_barrierorconcern
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_barrierorconcern> _logger;

        public dfe_barrierorconcern(BQProcessor processor, ILogger<dfe_barrierorconcern> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_barrierorconcern")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_barrierorconcern timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_barrierorconcern");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_barrierorconcern timer");
                throw;
            }
        }
    }
}

