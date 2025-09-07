using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_teachingcollegeappliedto
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_teachingcollegeappliedto> _logger;

        public dfe_teachingcollegeappliedto(BQProcessor processor, ILogger<dfe_teachingcollegeappliedto> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_teachingcollegeappliedto")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_teachingcollegeappliedto timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_teachingcollegeappliedto");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_teachingcollegeappliedto timer");
                throw;
            }
        }
    }
}