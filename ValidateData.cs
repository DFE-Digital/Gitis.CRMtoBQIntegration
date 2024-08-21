using Azure.Messaging.ServiceBus;
using Google;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using SendCRMChangesToBQ.DTO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SendCRMChangesToBQ
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
        public static Dictionary<string, List<string>> bigQueryCache = [];
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

            var validatedQueueSender = new ServiceBusClient(CrmToBqConnection).CreateSender("crmtobq");

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
                //string primarykey = entityName + "id";
                //var entityReference = context.MessageName == "Delete" ? (EntityReference)context.InputParameters["Target"] : null;
                var entity = context.MessageName != "Delete" ?  GetEntity(entityID,entityName) : null;

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
                    ServiceBusBQ serviceBusBQ = null;
                    switch (msgtype)
                    {
                        case "Create":
                        case "Update":
                            var entityFields = GetEntityAndFields(bigQueryClient, entity);
                            if (entityFields.Value.Count > 0)
                            {
                                serviceBusBQ = new ServiceBusBQ { MessageType = msgtype, Id = entityID, LogicalName = entityName, Fields = entityFields.Value };
                            }
                            break;
                        case "Delete":
                            serviceBusBQ = new ServiceBusBQ { MessageType = msgtype, Id = entityID, LogicalName = entityName, Fields = new List<Field>() };
                            break;
                    }
                    if(serviceBusBQ != null)
                    {
                        await validatedQueueSender.SendMessageAsync(new ServiceBusMessage(JsonConvert.SerializeObject(serviceBusBQ)));
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
                    await messageActions.DeadLetterMessageAsync(message, deadLetterReason: $"{msgtype} of {entityName} with id {entityID} failed",deadLetterErrorDescription: ex.Message);
                    _logger.LogInformation($"Dead lettered message : {message.MessageId}");
                   
                }
                catch (Exception deadLetterEx)
                {
                    _logger.LogCritical($"Failed to dead-letter message: {deadLetterEx.Message}");
                    throw;
                }                

            }        


        }

        private static dynamic GetValueForAttribute(string attribute, Entity entity)
        {
            var attributeValue = entity.Attributes.Contains(attribute) ? entity.Attributes[attribute] : null;
            if (attributeValue == null)
            {
                Console.WriteLine("Attribute or attribute value is null.");
                return null;
            }
            try
            {
                switch (attributeValue)
                {
                    case string strValue:
                        if (attributeValue.ToString().Contains("Date("))
                        {
                            string jsonDate = attributeValue.ToString();
                            long milliseconds = long.Parse(jsonDate.Substring(6, jsonDate.Length - 8));
                            return DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).DateTime.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
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
                        return attributeValue.ToString();
                    case DateTime dateTimeValue:
                        return ((DateTime)attributeValue).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
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
        public static KeyValuePair<string, List<Field>> GetEntityAndFields(BigQueryClient bigQueryClient, Entity entity)
        {
            var keyValuePair = new KeyValuePair<string, List<string>>();
            
            if (!bigQueryCache.ContainsKey(entity.LogicalName))
            {
                string queryBQ = $"SELECT column_name FROM `{projectId}.{datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{entity.LogicalName}' ORDER BY column_name";
                var results = bigQueryClient.CreateQueryJob(queryBQ, parameters: null).PollUntilCompleted();
                List<string> fields = [];

                if (results.GetQueryResults().TotalRows > 0)
                {
                    fields = results.GetQueryResults().Select(x => x["column_name"].ToString().ToLower()).ToList();
                    bigQueryCache.Add(entity.LogicalName, fields);
                }
                else
                {
                    return default;
                }
                
                keyValuePair = new KeyValuePair<string, List<string>>(entity.LogicalName, fields);
            }else
            {
                keyValuePair = bigQueryCache.FirstOrDefault(x => x.Key == entity.LogicalName);
            }
            var attributeValues = keyValuePair.Value.Where(x => entity.Attributes.Contains(x)).Select(x => { return new Field { Key = x, Value = GetValueForAttribute(x, entity) }; }).ToList();
            //attributeValues.Add(new Field() { Key = "id", Value = entity.Id.ToString() });
            return keyValuePair.Equals(default(KeyValuePair<string, List<string>>)) ? default : new KeyValuePair<string, List<Field>>(entity.LogicalName, attributeValues);
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

        private Entity GetEntity(string entityId, string entityName)
        {
            using (var svc = new ServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}"))
            {
                var entId = new Guid(entityId);

                var entity = svc.Retrieve(entityName, entId, new ColumnSet(true));
                _logger.LogInformation($"Entity - {entity.Id} retrieved...");
                return entity;

            }

        }
    }
}
