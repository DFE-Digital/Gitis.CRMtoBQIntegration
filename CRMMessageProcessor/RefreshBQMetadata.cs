using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SendToBQ
{
    public class RefreshBQMetadata
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
        private static readonly string entities = Environment.GetEnvironmentVariable("tablenames");

        //private static readonly string sbtopicname = Environment.GetEnvironmentVariable("sbtopicname");
        private readonly BigQueryClient _bigQueryClient;
        private readonly ILogger<RefreshBQMetadata> _logger;

        public RefreshBQMetadata(ILogger<RefreshBQMetadata> logger)
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

        [Function(nameof(RefreshBQMetadata))]
        public async Task Run([TimerTrigger("0 */30 * * * *")] TimerInfo timerInfo, FunctionContext context)
        {
            try
            {
                var configs = GetAllExistingBQConfigs();

                var tables = entities.Split(",");

                foreach (var table in tables)
                {
                    var configEntity = configs.FirstOrDefault(x => x["dfe_name"].ToString() == table);
                    List<string> fields = new List<string>();
                    if (configEntity == null || ((bool)configEntity["dfe_refreshfrombq"]))
                    {
                        string queryBQ = $"SELECT column_name FROM `{projectId}.{datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table}' ORDER BY column_name";
                        var results = await _bigQueryClient.CreateQueryJobAsync(queryBQ, parameters: null);

                        if (results.GetQueryResults().TotalRows > 0)
                        {
                            fields = results.GetQueryResults().Select(x => x["column_name"].ToString().ToLower()).ToList();
                        }
                        var keyValuePair = new KeyValuePair<string, List<string>>("", fields);

                        Entity newEntity = new Entity("dfe_bigquerytableconfig");
                        newEntity["dfe_tablejson"] = JsonConvert.SerializeObject(fields);
                        newEntity["dfe_name"] = table;
                        CreateConfig(newEntity);

                        if(configEntity != null)
                        {
                            DeleteConfig(configEntity);
                        }
                    }
                }

            }
            catch (Exception ex)
            {

                _logger.LogCritical($"Failed contact timer : {ex.Message}");
                throw;

            }

        }

        private static List<Entity> GetAllExistingBQConfigs() {
            using var svc = new ServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}");

            // Set Condition Values
            var query_statecode = 0;

            // Instantiate QueryExpression query
            var query = new QueryExpression("dfe_bigquerytableconfig");

            // Add columns to query.ColumnSet
            query.ColumnSet.AddColumns("dfe_name", "dfe_refreshfrombq", "dfe_tablejson");

            // Add conditions to query.Criteria
            query.Criteria.AddCondition("statecode", ConditionOperator.Equal, query_statecode);

            var response = svc.RetrieveMultiple(query);

            return response?.Entities.ToList();            

        }

        private static void CreateConfig(Entity entity)
        {
            using var svc = new ServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}");
            svc.Create(entity);
        }

        private static void DeleteConfig(Entity entity)
        {
            using var svc = new ServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}");
            svc.Delete(entity.LogicalName, entity.Id);
        }

    }
}
