using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_candidatequalification
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_candidatequalification> _logger;

        public dfe_candidatequalification(BQProcessor processor, ILogger<dfe_candidatequalification> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_candidatequalification")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_candidatequalification timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_candidatequalification");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_candidatequalification timer");
                throw;
            }
        }
    }
}