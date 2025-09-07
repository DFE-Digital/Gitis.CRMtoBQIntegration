using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_language
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_language> _logger;

        public dfe_language(BQProcessor processor, ILogger<dfe_language> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_language")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_language timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_language");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_language timer");
                throw;
            }
        }
    }
}