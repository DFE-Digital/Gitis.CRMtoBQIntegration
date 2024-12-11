using Azure.Messaging.ServiceBus;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using CRMMessageProcessor.DTO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CRMMessageProcessor
{
    public class ValidateData
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
        //public static Dictionary<string, List<string>> bigQueryCache = [];
        private readonly ILogger<ValidateData> _logger;

        public ValidateData(ILogger<ValidateData> logger)
        {
            _logger = logger;
        }

        [Function(nameof(ValidateData))]
        public async Task Run([ServiceBusTrigger("datafromcrm", "subscriptionbq", Connection = "sbconnection")] ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions)
        {
            _logger.LogInformation($"C# ServiceBus topic trigger function processed message: {message.Body.ToString()}");
            
            ServiceBusSender requeueSender = new ServiceBusClient(CrmToBqConnection).CreateSender("datafromcrm");

            string msgtype = "";
            string entityID = "";
            string entityName = "";
            try
            {
                var context = DeserializeJsonString<RemoteExecutionContext>(message.Body.ToString());

                _logger.LogInformation($" message name: {context.MessageName}");
                _logger.LogInformation($" Primary Entity ID: {context.PrimaryEntityId}");
                _logger.LogInformation($" Primary Entity Name: {context.PrimaryEntityName}");

                msgtype = context.MessageName;
                entityID = context.PrimaryEntityId.ToString();
                entityName = context.PrimaryEntityName;
                var validatedQueueSender = new ServiceBusClient(CrmToBqConnection).CreateSender(entityName);

                var entity = context.MessageName != "Delete" ? await GetEntity(entityID, entityName) : new Entity(context.PrimaryEntityName,context.PrimaryEntityId);

                var configEntity = await GetBigQueryConfig(context.PrimaryEntityName);                

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
                var bigQueryClient = await BigQueryClient.CreateAsync(projectId, googlecredentials);
                _logger.LogInformation($"BigQueryClient initiated for projectId {projectId}");

                ServiceBusBQ serviceBusBQ = null;
                var entityFields = new List<Field>();
                if (configEntity != null)
                {
                    switch (msgtype)
                    {
                        case "Create":
                        case "Update":
                            entityFields = await GetEntityAndFields(entity, configEntity);
                            if (entityFields.Count > 0)
                            {
                                serviceBusBQ = new ServiceBusBQ { MessageType = msgtype, Id = entityID, LogicalName = entityName, Fields = entityFields };
                                await validatedQueueSender.SendMessageAsync(new ServiceBusMessage(JsonConvert.SerializeObject(serviceBusBQ)));
                            }
                            break;
                        case "Delete":
                            entityFields.Add(new Field { Key = "id", Value = context.PrimaryEntityId });
                            entityFields.Add(new Field { Key = $"{context.PrimaryEntityName}id", Value = context.PrimaryEntityId });
                            entityFields.Add(new Field { Key = "statecode", Value = "Deleted" });
                            entityFields.Add(new Field { Key = "statuscode", Value = "Deleted" });
                            entityFields.Add(new Field { Key = "modifiedon", Value = DateTime.UtcNow.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'") });
                            entityFields.Add(new Field { Key = "modifiedby", Value = context.UserId.ToString() });
                            serviceBusBQ = new ServiceBusBQ { MessageType = msgtype, Id = entityID, LogicalName = entityName, Fields = entityFields };
                            await validatedQueueSender.SendMessageAsync(new ServiceBusMessage(JsonConvert.SerializeObject(serviceBusBQ)));
                            break;
                    }
                }

            }            
            catch (Exception ex)
            {
                _logger.LogCritical($"Message failed");
                Random rng = new();
                await requeueSender.ScheduleMessageAsync(new ServiceBusMessage(message), DateTime.UtcNow.AddMinutes(rng.Next(1, 30)));

            }

        }

        private static dynamic GetValueForAttribute(string attribute, Entity entity)
        {
            var attributeValue = attribute == "id" ? entity.Id : entity.Attributes.Contains(attribute) ? entity.Attributes[attribute] : null;
            if (attributeValue == null)
            {                
                return null;
            }            
            try
            {
                switch (attributeValue)
                {
                    case string strValue:
                        if (attributeValue.ToString().Contains("Date("))
                        {
                            string jsonDate = attributeValue.ToString().Replace("Date(", "").Replace(")", "").Split("+")[0];
                            long milliseconds = long.Parse(jsonDate);
                            return DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).DateTime.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
                        }
                        else
                        {
                            string resultValue = Regex.Replace(attributeValue.ToString(), @"\t|\n|\r", " ");

                            return resultValue.Replace("'", @"\'");
                        }
                    case int intValue:
                        return (int)attributeValue;
                    case decimal decimalValue:
                        return (decimal)attributeValue;
                    case bool boolValue:
                        return entity.FormattedValues[attribute];
                    case Guid guidValue:
                        return guidValue.ToString();
                    case DateTime dateTimeValue:
                        return ((DateTime)attributeValue).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
                    case EntityReference entityReferenceValue:
                        return ((EntityReference)attributeValue).Id.ToString();
                    case OptionSetValue optionSetValue:
                        var osLabel = entity.FormattedValues[attribute];
                        if (osLabel != null)
                        {
                            return osLabel;
                        }
                        else
                        {
                            return ((OptionSetValue)attributeValue).Value;
                        }
                    case Money moneyValue:
                        return ((Money)attributeValue).Value;
                    case EntityCollection entityCollection:
                        if (((EntityCollection)attributeValue).Entities.Count > 0)
                        {
                            foreach (var attributeName in ((EntityCollection)attributeValue).Entities[0].Attributes)
                            {
                                if (attributeName.Key == "partyid")
                                {
                                    return ((EntityReference)attributeName.Value).Id.ToString();
                                }
                            }
                        }
                        return null;
                    default:
                        return null;
                }

            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public static async Task<List<Field>> GetEntityAndFields(Entity entity, Entity configEntity)
        {

            List<string> fields = new List<string>();

            fields = DeserializeJsonString<List<string>>(configEntity.GetAttributeValue<string>("dfe_tablejson"));

            var keyValuePair = new KeyValuePair<string, List<string>>(entity.LogicalName, fields);

            var attributeValues = keyValuePair.Value.Where(x => (entity.Attributes.Contains(x) || x == "id")).Select(x => { return new Field { Key = x, Value = GetValueForAttribute(x, entity) }; }).ToList();

            return attributeValues;
        }

        public static RemoteContextType DeserializeJsonString<RemoteContextType>(string jsonString)
        {
            //create an instance of generic type object
            RemoteContextType obj = Activator.CreateInstance<RemoteContextType>();
            MemoryStream ms = new MemoryStream(Encoding.Unicode.GetBytes(jsonString));
            System.Runtime.Serialization.Json.DataContractJsonSerializer serializer = new System.Runtime.Serialization.Json.DataContractJsonSerializer(obj.GetType());
            obj = (RemoteContextType)serializer.ReadObject(ms);
            ms.Close();
            return obj;
        }

        private static void CreateConfig(Entity entity)
        {
            using var svc = new ServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}");
            svc.Create(entity);
        }

        private async Task<Entity> GetBigQueryConfig(string entityName)
        {
            using var svc = new ServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}");

            var query = new QueryExpression("dfe_bigquerytableconfig");

            query.ColumnSet.AllColumns = true;

            query.Criteria.AddCondition("dfe_name", ConditionOperator.Equal, entityName);

            var entityCollection = await svc.RetrieveMultipleAsync(query);
            //_logger.LogInformation($"{entityCollection.TotalRecordCount} {entityName} config retrieved...");

            if (entityCollection?.Entities.Count > 0)
            {
                return entityCollection.Entities.First();
            }
            else
            {
                return null;
            }
        }

        private async Task<Entity> GetEntity(string entityId, string entityName)
        {
            using (var svc = new ServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}"))
            {               

                var entId = new Guid(entityId);

                var entity = await svc.RetrieveAsync(entityName, entId, new ColumnSet(true));
                _logger.LogInformation($"Entity - {entity.Id} retrieved...");
                return entity;

            }

        }
    }
}
