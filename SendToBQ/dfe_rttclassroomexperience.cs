using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_rttclassroomexperience
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_rttclassroomexperience> _logger;

        public dfe_rttclassroomexperience(BQProcessor processor, ILogger<dfe_rttclassroomexperience> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_rttclassroomexperience")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_rttclassroomexperience timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_rttclassroomexperience");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_rttclassroomexperience timer");
                throw;
            }
        }
    }
}