using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_calltopics
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_calltopics> _logger;

        public dfe_calltopics(BQProcessor processor, ILogger<dfe_calltopics> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_calltopics")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_calltopics timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_calltopics");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_calltopics timer");
                throw;
            }
        }
    }
}

