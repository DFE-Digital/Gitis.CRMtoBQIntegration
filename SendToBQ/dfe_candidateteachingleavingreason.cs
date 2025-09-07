using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_candidateteachingleavingreason
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_candidateteachingleavingreason> _logger;

        public dfe_candidateteachingleavingreason(BQProcessor processor, ILogger<dfe_candidateteachingleavingreason> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_candidateteachingleavingreason")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_candidateteachingleavingreason timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_candidateteachingleavingreason");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_candidateteachingleavingreason timer");
                throw;
            }
        }
    }
}