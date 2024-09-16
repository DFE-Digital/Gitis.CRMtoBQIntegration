using Azure.Messaging.ServiceBus;
using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SendToBQ.DTO;
using System;
using System.Linq;
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

            int minutesToWait = 30;

            string msgType = "";
            string entityID = "";
            string entityName = "";

            var context = JsonConvert.DeserializeObject<ServiceBusBQ>(message.Body.ToString());

            _logger.LogInformation($" message name: {context.MessageType}");
            _logger.LogInformation($" Primary Entity ID: {context.Id}");
            _logger.LogInformation($" Primary Entity Name: {context.LogicalName}");

            msgType = context.MessageType;
            entityID = context.Id;
            entityName = context.LogicalName;
            string primarykey = entityName + "id";

            ServiceBusSender requeueSender = new ServiceBusClient(CrmToBqConnection).CreateSender(entityName);

            try
            {
                var row = new BigQueryInsertRow();

                string bQuery = "";

                if (msgType == "Create")
                {
                    var keyValueFields = context.Fields.ToDictionary(x => x.Key, x => x.Value);
                    row.Add(keyValueFields);
                    await _bigQueryClient.InsertRowAsync(projectId, datasetId, entityName, row, null);
                }
                else if (msgType == "Update")
                {
                    try
                    {
                        bQuery = $"Delete from `{projectId}.{datasetId}.{entityName}` where Id = '{entityID}'";

                        await _bigQueryClient.ExecuteQueryAsync(bQuery, null);
                        var keyValueFields = context.Fields.ToDictionary(x => x.Key, x => x.Value);

                        row.Add(keyValueFields);
                        var table = _bigQueryClient.GetTable(datasetId, entityName);
                        await table.InsertRowAsync(row);
                    }
                    catch (Exception ex)
                    {
                        if (ex.ToString().Contains("would affect rows in the streaming buffer"))
                        {

                            await requeueSender.ScheduleMessageAsync(new ServiceBusMessage(message), DateTime.UtcNow.AddMinutes(minutesToWait));

                        }
                        else if (ex.ToString().Contains("concurrent") || ex.ToString().Contains("DML statements outstanding") || ex.ToString().Contains("table dml") || ex.ToString().Contains("socket") || ex.ToString().Contains("A connection attempt failed"))
                        {
                            await requeueSender.ScheduleMessageAsync(new ServiceBusMessage(message), DateTime.UtcNow.AddMinutes(minutesToWait));
                        }
                        else
                        {
                            _logger.LogCritical($"ServiceBus topic trigger function - See message :- {ex.Message}");
                            throw;
                        }

                    }
                }
                else if (msgType == "Delete")
                {
                    try
                    {
                        bQuery = $"Delete from `{projectId}.{datasetId}.{entityName}` where Id = '{entityID}'";
                        await _bigQueryClient.ExecuteQueryAsync(bQuery, null);
                        var keyValueFields = context.Fields.ToDictionary(x => x.Key, x => x.Value);
                    }
                    catch (Exception ex)
                    {
                        if (ex.ToString().Contains("would affect rows in the streaming buffer"))
                        {
                            await requeueSender.ScheduleMessageAsync(new ServiceBusMessage(message), DateTime.UtcNow.AddMinutes(minutesToWait));

                        }
                        else if (ex.ToString().Contains("concurrent") || ex.ToString().Contains("DML statements outstanding") || ex.ToString().Contains("table dml") || ex.ToString().Contains("socket") || ex.ToString().Contains("A connection attempt failed"))
                        {
                            await requeueSender.ScheduleMessageAsync(new ServiceBusMessage(message), DateTime.UtcNow.AddMinutes(minutesToWait));
                        }
                        else
                        {
                            _logger.LogCritical($"ServiceBus topic trigger function - See message :- {ex.Message}");
                            throw;
                        }
                    }
                }

            }
            catch (JsonSerializationException e)
            {
                _logger.LogCritical($"Entity Name: {entityName}");
                _logger.LogCritical(e.ToString());
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"Entity Name: {entityName}");
                _logger.LogCritical(ex.ToString());
                throw;
            }



        }
    }
}
