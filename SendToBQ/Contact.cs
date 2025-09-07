using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class Contact
    {
        private readonly BQProcessor _processor;
        private readonly ILogger<Contact> _logger;

        public Contact(BQProcessor processor, ILogger<Contact> logger)
        {
            _processor = processor;
            _logger = logger;
        }

        [Function("contact")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo)
        {
            try
            {
                _logger.LogInformation("Contact timer fired at {utc}", DateTimeOffset.UtcNow);
                await _processor.Process("contact");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed contact timer");
                throw;
            }
        }
    }
}