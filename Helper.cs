using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace CRMChangestoBQIntegration
{
    internal class RetryHelper
    {
        public static async Task RetryMessageAsync(ServiceBusMessage message, ServiceBusSender messageSender, ILogger log, int minutesToWait)
        {
            try
            {

                var clonedsbmsg = new ServiceBusMessage(message.Body);
                clonedsbmsg.ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(minutesToWait);
                await messageSender.ScheduleMessageAsync(clonedsbmsg, clonedsbmsg.ScheduledEnqueueTime);
                log.LogInformation($"Successfully scheduled {message.MessageId} in the queue");

            }
            catch (Exception exception)
            {
                log.LogCritical($"ServiceBus topic trigger function - See error message :- {exception.Message}");
                throw;
            }
        }
    }
    }

