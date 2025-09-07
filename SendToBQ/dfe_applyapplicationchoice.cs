using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_applyapplicationchoice
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_applyapplicationchoice> _logger;

        public dfe_applyapplicationchoice(BQProcessor processor, ILogger<dfe_applyapplicationchoice> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_applyapplicationchoice")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_applyapplicationchoice timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_applyapplicationchoice");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed contact timer");
                throw;
            }
        }
    }
}


