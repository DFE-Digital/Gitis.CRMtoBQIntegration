using Azure.Messaging.ServiceBus;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SendToBQ.DTO;
using SendToBQ.Processor;
using System;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class dfe_applyapplicationchoice
    {

        private static readonly string projectId = Environment.GetEnvironmentVariable("projectId");
        private static readonly string datasetId = Environment.GetEnvironmentVariable("datasetId");
        
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
        
        
        private static readonly string sbtopicname = Environment.GetEnvironmentVariable("sbtopicname");
        private readonly BigQueryClient _bigQueryClient;
        private readonly ILogger<dfe_applyapplicationchoice> _logger;

        public dfe_applyapplicationchoice(ILogger<dfe_applyapplicationchoice> logger)
        {
            _logger = logger;
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
            _bigQueryClient = BigQueryClient.Create(projectId, googlecredentials);
            _logger.LogInformation($"BigQueryClient initiated for projectId {projectId}");
        }

        [Function(nameof(dfe_applyapplicationchoice))]
        public async Task Run([ServiceBusTrigger("dfe_applyapplicationchoice", Connection = "CrmToBqConnection")] ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions)
        {
            _logger.LogInformation($"C# ServiceBus topic trigger function processed message: {message.Body.ToString()}");
            string msgType = "";
            string entityID = "";
            string entityName = "";            
                        
            try {

                var context = JsonConvert.DeserializeObject<ServiceBusBQ>(message.Body.ToString());
                msgType = context.MessageType;
                entityID = context.Id;
                entityName = context.LogicalName;

                BQProcessor processor = new(_bigQueryClient, _logger);
                await processor.Process(message);

            }
            catch (Exception ex)
            {
                try
                {

                    _logger.LogInformation($"Dead lettering message : {message.MessageId}");
                    await messageActions.DeadLetterMessageAsync(message, deadLetterReason: $"{msgType} of {entityName} with id {entityID} failed", deadLetterErrorDescription: ex.Message);
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
