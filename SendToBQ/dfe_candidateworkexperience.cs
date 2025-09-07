using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_candidateworkexperience
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_candidateworkexperience> _logger;

        public dfe_candidateworkexperience(BQProcessor processor, ILogger<dfe_candidateworkexperience> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_candidateworkexperience")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_candidateworkexperience timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_candidateworkexperience");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_candidateworkexperience timer");
                throw;
            }
        }
    }
}