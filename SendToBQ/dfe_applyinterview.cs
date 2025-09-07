using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_applyinterview
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_applyinterview> _logger;

        public dfe_applyinterview(BQProcessor processor, ILogger<dfe_applyinterview> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_applyinterview")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_applyinterview timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_applyinterview");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_applyinterview timer");
                throw;
            }
        }
    }
}

