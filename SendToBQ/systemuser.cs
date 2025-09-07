using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class systemuser
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<systemuser> _logger;

        public systemuser(BQProcessor processor, ILogger<systemuser> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("systemuser")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("systemuser timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("systemuser");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed systemuser timer");
                throw;
            }
        }
    }
}