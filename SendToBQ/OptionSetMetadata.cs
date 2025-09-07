using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class OptionSetMetadata
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<OptionSetMetadata> _logger;

        public OptionSetMetadata(BQProcessor processor, ILogger<OptionSetMetadata> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("OptionSetMetadata")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("OptionSetMetadata timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("OptionSetMetadata");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed OptionSetMetadata timer");
                throw;
            }
        }
    }
}