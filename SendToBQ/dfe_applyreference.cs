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
    public class dfe_applyreference
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
        private readonly ILogger<dfe_applyreference> _logger;

        public dfe_applyreference(ILogger<dfe_applyreference> logger)
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

        [Function(nameof(dfe_applyreference))]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo, FunctionContext context)
        {
            try
            {

                BQProcessor processor = new(_bigQueryClient, _logger);
                await processor.Process("dfe_applyreference");

            }
            catch (Exception ex)
            {

                _logger.LogCritical($"Failed {nameof(dfe_applyreference)} timer : {ex.Message}");
                throw;

            }

        }

    }
}
