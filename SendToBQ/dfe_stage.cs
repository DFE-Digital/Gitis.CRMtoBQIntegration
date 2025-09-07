using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_stage
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_stage> _logger;

        public dfe_stage(BQProcessor processor, ILogger<dfe_stage> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_stage")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_stage timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_stage");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_stage timer");
                throw;
            }
        }
    }
}