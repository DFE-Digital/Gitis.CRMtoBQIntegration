using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_candidatepreferredcontacttime
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_candidatepreferredcontacttime> _logger;

        public dfe_candidatepreferredcontacttime(BQProcessor processor, ILogger<dfe_candidatepreferredcontacttime> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_candidatepreferredcontacttime")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_candidatepreferredcontacttime timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_candidatepreferredcontacttime");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_candidatepreferredcontacttime timer");
                throw;
            }
        }
    }
}