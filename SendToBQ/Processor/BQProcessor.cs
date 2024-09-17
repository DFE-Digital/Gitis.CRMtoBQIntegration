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
            ServiceBusReceiver serviceBusReceiver = new ServiceBusClient(CrmToBqConnection).CreateReceiver(queueName);

            var messages = await serviceBusReceiver.ReceiveMessagesAsync(100);            

            string msgType = "";
            string entityID = "";
            string entityName = "";           

            try
            {
                var rows = new List<BigQueryInsertRow>();
                foreach (var message in messages)
                {
                    var context = JsonConvert.DeserializeObject<ServiceBusBQ>(message.Body.ToString());
                    msgType = context.MessageType;
                    entityID = context.Id;
                    entityName = context.LogicalName;
                    string primarykey = entityName + "id";

                    _logger.LogInformation($" message name: {context.MessageType}");
                    _logger.LogInformation($" Primary Entity ID: {context.Id}");
                    _logger.LogInformation($" Primary Entity Name: {context.LogicalName}");

                    if (msgType == "Create" || msgType == "Update")
                    {
                        var keyValueFields = context.Fields.ToDictionary(x => x.Key, x => x.Value);
                        rows.Add([keyValueFields]);

                    }
                    else if (msgType == "Delete")
                    {
                        var keyValueFields = context.Fields.ToDictionary(x => x.Key, x => x.Value);
                        rows.Add([keyValueFields]);
                    }
                }

                await _bigQueryClient.InsertRowsAsync(projectId, datasetId, entityName, rows.ToArray());

                foreach(var message in messages)
                {
                    await serviceBusReceiver.CompleteMessageAsync(message);
                }

            }
            catch (Exception ex)
            {
                foreach (var message in messages)
                {
                    await serviceBusReceiver.DeadLetterMessageAsync(message,ex.Message,ex.StackTrace);
                }
                _logger.LogCritical($"Entity Name: {queueName}");
                _logger.LogCritical(ex.ToString());
                throw;
            }



        }
    }
}
