using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class GlobalOptionSetMetadata
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<GlobalOptionSetMetadata> _logger;

        public GlobalOptionSetMetadata(BQProcessor processor, ILogger<GlobalOptionSetMetadata> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("GlobalOptionSetMetadata")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("GlobalOptionSetMetadata timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("GlobalOptionSetMetadata");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed GlobalOptionSetMetadata timer");
                throw;
            }
        }
    }
}