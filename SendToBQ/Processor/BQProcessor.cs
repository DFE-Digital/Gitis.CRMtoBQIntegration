using Azure.Messaging.ServiceBus;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SendToBQ.DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Channels;
using System.Threading.Tasks;

namespace SendToBQ.Processor
{
    public class BQProcessor
    {
        //private static readonly string CrmToBqConnection = Environment.GetEnvironmentVariable("CrmToBqConnection");
        private static readonly string projectId = Environment.GetEnvironmentVariable("projectId");
        private static readonly string datasetId = Environment.GetEnvironmentVariable("datasetId");
        private readonly BigQueryClient _bq;
        private readonly ILogger _log;      
        private readonly ServiceBusFactory _sb;

        public BQProcessor(BigQueryClient bigQueryClient, ServiceBusFactory sbFactory, ILogger<BQProcessor> logger)
        {
            _bq = bigQueryClient;
            _sb = sbFactory;
            _log = logger;
        }
        public async Task Process(string queueName)
        {
            var receiver = _sb.GetReceiver(queueName);
            var messages = await receiver.ReceiveMessagesAsync(1000);


            ServiceBusSender serviceBusSender = _sb.GetSender(queueName);

            //var messages = await serviceBusReceiver.ReceiveMessagesAsync(100);
           
            _log.LogInformation($" messages count: {messages.Count}");

            try
            {
                var rows = new List<BigQueryInsertRow>();
                foreach (var message in messages)
                {
                    var context = JsonConvert.DeserializeObject<ServiceBusBQ>(message.Body.ToString());                    

                    try
                    {                      

                        var keyValueFields = context.Fields.ToDictionary(x => x.Key, x => x.Value);
                        rows.Add([keyValueFields]);
                    }
                    catch (Exception ex)
                    {
                        await receiver.DeadLetterMessageAsync(message, $"Prevalidate - {ex.Message}", ex.StackTrace);
                    }
                }
                InsertOptions options = new()
                {
                    SkipInvalidRows = true
                };

                var bigQueryInsertResult = await _bq.InsertRowsAsync(projectId, datasetId, queueName, rows.ToArray(), options);

                if (bigQueryInsertResult.Status == BigQueryInsertStatus.SomeRowsInserted)
                {
                    var errors = bigQueryInsertResult.Errors.ToList();

                    _log.LogCritical($"Errors: {errors.Count()}");

                    for (int i = 0; messages.Count < i; i++)
                    {
                        if (errors.Any(x => x.OriginalRowIndex == i))
                        {
                            var failedMessage = messages[i];

                            _log.LogCritical($"Error message index: {i}");
                            foreach (var row in errors.FirstOrDefault(x => x.OriginalRowIndex == i))
                            {
                                
                                await serviceBusSender.SendMessageAsync(new ServiceBusMessage(failedMessage));
                            }
                        }
                        else
                        {
                            var message = messages[i];
                            await receiver.CompleteMessageAsync(message);
                        }

                    }
                    
                }
                else if (bigQueryInsertResult.Status == BigQueryInsertStatus.NoRowsInserted)
                {
                    foreach (var message in messages)
                    {
                        await serviceBusSender.SendMessageAsync(new ServiceBusMessage(message));
                    }
                }
                else
                {
                    foreach (var message in messages)
                    {
                        await receiver.CompleteMessageAsync(message);
                    }
                }
                

            }
            catch (Exception ex)
            {
                _log.LogCritical($"Entity Name: {queueName}");
                _log.LogCritical(ex.ToString());
                throw;
            }



        }
    }
}
