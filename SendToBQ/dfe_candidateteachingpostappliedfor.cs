using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_candidateteachingpostappliedfor
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_candidateteachingpostappliedfor> _logger;

        public dfe_candidateteachingpostappliedfor(BQProcessor processor, ILogger<dfe_candidateteachingpostappliedfor> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_candidateteachingpostappliedfor")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_candidateteachingpostappliedfor timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_candidateteachingpostappliedfor");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_candidateteachingpostappliedfor timer");
                throw;
            }
        }
    }
}