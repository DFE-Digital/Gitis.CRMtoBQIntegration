using Azure.Messaging.ServiceBus;
using Google;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SendCRMChangesToBQ.DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SendCRMChangesToBQ
{
    public class SendToBQ
    {

        private static readonly string projectId = Environment.GetEnvironmentVariable("projectId");
        private static readonly string datasetId = Environment.GetEnvironmentVariable("datasetId");
        private static readonly string d365Environment = Environment.GetEnvironmentVariable("d365Environment");
        private static readonly string sUserKey = Environment.GetEnvironmentVariable("userkey");
        private static readonly string sUserPassword = Environment.GetEnvironmentVariable("userpassword");
        private static readonly string clientid = Environment.GetEnvironmentVariable("clientId");
        private static readonly string clientsecret = Environment.GetEnvironmentVariable("clientSecret");
        private static readonly string gtype = Environment.GetEnvironmentVariable("googlecredentials:type");
        private static readonly string gproject_id = Environment.GetEnvironmentVariable("googlecredentials:project_id");
        private static readonly string gprivate_key_id = Environment.GetEnvironmentVariable("googlecredentials:private_key_id");
        private static readonly string gprivate_key = Environment.GetEnvironmentVariable("googlecredentials:private_key");
        private static readonly string gclient_email = Environment.GetEnvironmentVariable("googlecredentials:client_email");
        private static readonly string gclient_id = Environment.GetEnvironmentVariable("googlecredentials:client_id");
        private static readonly string gauth_uri = Environment.GetEnvironmentVariable("googlecredentials:auth_uri");
        private static readonly string gtoken_uri = Environment.GetEnvironmentVariable("googlecredentials:token_uri");
        private static readonly string gauth_provider_x509_cert_url = Environment.GetEnvironmentVariable("googlecredentials:auth_provider_x509_cert_url");
        private static readonly string gclient_x509_cert_url = Environment.GetEnvironmentVariable("googlecredentials:client_x509_cert_url");
        private static readonly string sbconnection = Environment.GetEnvironmentVariable("sbconnection");
        private static readonly string CrmToBqConnection = Environment.GetEnvironmentVariable("CrmToBqConnection");
        private static readonly string sbtopicname = Environment.GetEnvironmentVariable("sbtopicname");
        public static Dictionary<string, List<string>> bigQueryCache = [];
        private readonly ILogger<SendToBQ> _logger;

        public SendToBQ(ILogger<SendToBQ> logger)
        {
            _logger = logger;
        }

        [Function(nameof(SendToBQ))]
        public async Task Run([ServiceBusTrigger("crmtobq", Connection = "CrmToBqConnection")] ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions, ServiceBusClient serviceBusClient)
        {
            _logger.LogInformation($"C# ServiceBus topic trigger function processed message: {message.Body.ToString()}");

            int minutesToWait = 30;

            ServiceBusSender requeueSender = new ServiceBusClient(CrmToBqConnection).CreateSender("crmtobq");

            string msgtype = "";
            string entityID = "";
            string entityName = "";
            try
            {
                var context = JsonConvert.DeserializeObject<ServiceBusBQ>(message.Body.ToString());

                _logger.LogInformation($" message name: {context.MessageType}");
                _logger.LogInformation($" Primary Entity ID: {context.Id}");
                _logger.LogInformation($" Primary Entity Name: {context.LogicalName}");

                msgtype = context.MessageType;
                entityID = context.Id;
                entityName = context.LogicalName;
                string primarykey = entityName + "id";

                var param = new JsonCredentialParameters
                {
                    Type = gtype,
                    ProjectId = gproject_id,
                    PrivateKeyId = gprivate_key_id,
                    PrivateKey = gprivate_key,
                    ClientEmail = gclient_email,
                    ClientId = gclient_id,
                    TokenUrl = gtoken_uri
                };

                var googlecredentials = GoogleCredential.FromJsonParameters(param);
                var bigQueryClient = BigQueryClient.Create(projectId, googlecredentials);                
                _logger.LogInformation($"BigQueryClient initiated for projectId {projectId}");

                try
                {
                    var row = new BigQueryInsertRow();

                    string bQuery = "";
                    
                    if (msgtype == "Create")
                    {
                        var keyValueFields = context.Fields.ToDictionary(x => x.Key, x => x.Value);
                        row.Add(keyValueFields);
                        await bigQueryClient.InsertRowAsync(projectId, datasetId, entityName, row, null);
                    }
                    else if (msgtype == "Update")
                    {
                        try
                        {
                            bQuery = $"Delete from `{projectId}.{datasetId}.{entityName}` where Id = '{entityID}'";
                            BigQueryParameter[] parameters = null;

                            //BigQueryJob job = bigQueryClient.CreateQueryJob(bQuery, parameters);
                            //job.PollUntilCompleted().ThrowOnFatalError();
                            await bigQueryClient.ExecuteQueryAsync(bQuery, null);

                            var keyValueFields = context.Fields.ToDictionary(x => x.Key, x => x.Value);
                            
                            row.Add(keyValueFields);
                            var table = bigQueryClient.GetTable(datasetId, entityName);
                            //await bigQueryClient.InsertRowAsync(projectId, datasetId, entityName, row, null);
                            await table.InsertRowAsync(row);
                        }
                        catch (GoogleApiException ex)
                        {
                            if (ex.ToString().Contains("would affect rows in the streaming buffer"))
                            {

                                await requeueSender.ScheduleMessageAsync(new ServiceBusMessage(message.Body.ToString()) { ContentType = message.ContentType, To = message.To, Subject = message.Subject }, DateTime.UtcNow.AddMinutes(minutesToWait));
                                _logger.LogInformation($"Successfully scheduled {message.MessageId} in the queue");
                            }
                            else if (ex.ToString().Contains("concurrent") || ex.ToString().Contains("DML statements outstanding") || ex.ToString().Contains("table dml"))
                            {
                                await requeueSender.SendMessageAsync(new ServiceBusMessage(message.Body.ToString()));
                            }
                            else
                            {
                                _logger.LogCritical($"ServiceBus topic trigger function - See message :- {ex.Message}");
                                throw;
                            }

                        }
                    }
                    else if (msgtype == "Delete")
                    {
                        try
                        {
                            bQuery = $"Delete from `{projectId}.{datasetId}.{entityName}` where Id = '{entityID}'";
                            BigQueryParameter[] parameters = null;

                            BigQueryJob job = bigQueryClient.CreateQueryJob(bQuery, parameters);
                            job.PollUntilCompleted().ThrowOnFatalError();


                        }
                        catch (GoogleApiException ex)
                        {
                            if (ex.ToString().Contains("would affect rows in the streaming buffer"))
                            {
                                await requeueSender.ScheduleMessageAsync(new ServiceBusMessage(message.Body.ToString()) { ContentType = message.ContentType, To = message.To, Subject = message.Subject }, DateTime.UtcNow.AddMinutes(minutesToWait));
                                _logger.LogInformation($"Successfully scheduled {message.MessageId} in the queue");
                            }
                            else if (ex.ToString().Contains("concurrent") || ex.ToString().Contains("DML statements outstanding") || ex.ToString().Contains("table dml"))
                            {
                                await requeueSender.SendMessageAsync(new ServiceBusMessage(message.Body.ToString()));
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
            catch (Exception ex)
            {
                try
                {

                    _logger.LogInformation($"Dead lettering message : {message.MessageId}");
                    await messageActions.DeadLetterMessageAsync(message, deadLetterReason: $"{msgtype} of {entityName} with id {entityID} failed", deadLetterErrorDescription: ex.Message);
                    _logger.LogInformation($"Dead lettered message : {message.MessageId}");

                }
                catch (Exception deadLetterEx)
                {
                    _logger.LogCritical($"Failed to dead-letter message: {deadLetterEx.Message}");
                    throw;
                }

            }


        }
        
    }
}
