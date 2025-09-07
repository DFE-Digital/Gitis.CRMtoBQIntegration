using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_candidatejourneystep
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_candidatejourneystep> _logger;

        public dfe_candidatejourneystep(BQProcessor processor, ILogger<dfe_candidatejourneystep> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_candidatejourneystep")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_candidatejourneystep timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_candidatejourneystep");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_candidatejourneystep timer");
                throw;
            }
        }
    }
}