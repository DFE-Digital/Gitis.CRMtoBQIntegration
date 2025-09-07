using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_candidatetrainedtaughtteachingsubject
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<dfe_candidatetrainedtaughtteachingsubject> _logger;

        public dfe_candidatetrainedtaughtteachingsubject(BQProcessor processor, ILogger<dfe_candidatetrainedtaughtteachingsubject> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("dfe_candidatetrainedtaughtteachingsubject")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("dfe_candidatetrainedtaughtteachingsubject timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("dfe_candidatetrainedtaughtteachingsubject");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed dfe_candidatetrainedtaughtteachingsubject timer");
                throw;
            }
        }
    }
}