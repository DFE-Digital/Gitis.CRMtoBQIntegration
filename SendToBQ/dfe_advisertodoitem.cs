using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_advisertodoitem
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_advisertodoitem> _logger;

        public dfe_advisertodoitem(BQProcessor processor, ILogger<dfe_advisertodoitem> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_advisertodoitem")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_advisertodoitem timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_advisertodoitem");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed contact timer");
                throw;
            }
        }
    }
}


