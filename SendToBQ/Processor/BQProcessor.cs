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
        private static readonly string CrmToBqConnection = Environment.GetEnvironmentVariable("CrmToBqConnection");
        private static readonly string projectId = Environment.GetEnvironmentVariable("projectId");
        private static readonly string datasetId = Environment.GetEnvironmentVariable("datasetId");
        private readonly BigQueryClient _bigQueryClient;
        private readonly ILogger _logger;
        public BQProcessor(BigQueryClient bigQueryClient, ILogger logger) { 
        
            _bigQueryClient = bigQueryClient;
            _logger = logger;
        }
        public async Task Process(string queueName)
        {
            ServiceBusClient sbClient = new ServiceBusClient(CrmToBqConnection);

            ServiceBusReceiver serviceBusReceiver = sbClient.CreateReceiver(queueName);

            ServiceBusSender serviceBusSender = sbClient.CreateSender(queueName);

            var messages = await serviceBusReceiver.ReceiveMessagesAsync(100);
           
            _logger.LogInformation($" messages count: {messages.Count}");

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
                        //await serviceBusReceiver.DeadLetterMessageAsync(message, $"Prevalidate - {ex.Message}", ex.StackTrace);
                        await serviceBusSender.SendMessageAsync(new ServiceBusMessage(message));
                    }
                }
                InsertOptions options = new()
                {
                    SkipInvalidRows = true
                };

                var bigQueryInsertResult = await _bigQueryClient.InsertRowsAsync(projectId, datasetId, queueName, rows.ToArray(), options);

                if (bigQueryInsertResult.Status == BigQueryInsertStatus.SomeRowsInserted)
                {
                    var errors = bigQueryInsertResult.Errors.ToList();

                    _logger.LogCritical($"Errors: {errors.Count()}");

                    for (int i = 0; messages.Count < i; i++)
                    {
                        if (errors.Any(x => x.OriginalRowIndex == i))
                        {
                            var failedMessage = messages[i];

                            _logger.LogCritical($"Error message index: {i}");
                            foreach (var row in errors.FirstOrDefault(x => x.OriginalRowIndex == i))
                            {
                                
                                await serviceBusSender.SendMessageAsync(new ServiceBusMessage(failedMessage));
                            }
                        }
                        else
                        {
                            var message = messages[i];
                            await serviceBusReceiver.CompleteMessageAsync(message);
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
                        await serviceBusReceiver.CompleteMessageAsync(message);
                    }
                }
                

            }
            catch (Exception ex)
            {
                _logger.LogCritical($"Entity Name: {queueName}");
                _logger.LogCritical(ex.ToString());
                throw;
            }



        }
    }
}
