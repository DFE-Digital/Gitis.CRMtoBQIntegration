using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class team
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<team> _logger;

        public team(BQProcessor processor, ILogger<team> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("team")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("team timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("team");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed team timer");
                throw;
            }
        }
    }
}