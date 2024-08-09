using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System.Text.RegularExpressions;
using Azure.Messaging.ServiceBus;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Azure;

namespace CRMChangestoBQIntegration
{
    public class SBMessageProcess
    {
        //private readonly ILogger _logger;
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
        private static readonly string sbtopicname = Environment.GetEnvironmentVariable("sbtopicname");
        private static ServiceBusSender sbsender;
        private static ServiceBusClient sbclient;
        private static ILogger _logger;
        

        [Function("SBMessageProcess")]
        public static async Task Run([ServiceBusTrigger("datafromcrm", "subscriptionbq-pr", Connection = "sbconnection")] string serviceBusMessage, FunctionContext context)
        {
            _logger = context.GetLogger(nameof(SBMessageProcess));
            int minutesToWait = 30; //set the waiting time that service bus holds message
            sbclient = new ServiceBusClient(sbconnection);
            sbsender = sbclient.CreateSender(sbtopicname);
            
            try
            {

                _logger.LogInformation($"messageBody: {serviceBusMessage}");
                dynamic lparsedmsg = JsonConvert.DeserializeObject(serviceBusMessage);

                _logger.LogInformation($" message name: {lparsedmsg.MessageName}");
                _logger.LogInformation($" Primary Entity ID: {lparsedmsg.PrimaryEntityId}");
                _logger.LogInformation($" Primary Entity Name: {lparsedmsg.PrimaryEntityName}");

                string msgtype = lparsedmsg.MessageName;
                string entityID = lparsedmsg.PrimaryEntityId;
                string entityName = lparsedmsg.PrimaryEntityName;
                string primarykey = entityName + "id";

                

                //Start Connect Big Query
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
                BigQueryClient client = BigQueryClient.Create(projectId, googlecredentials);
                _logger.LogInformation($" BigQueryClient initiated for projectId {projectId}");

                List<BigQueryTable> tables = client.ListTables(datasetId).ToList();
                bool isTableinBQ = false;
                foreach (BigQueryTable table in tables)
                {
                    if (table.ToString().Contains(entityName))
                    {
                        isTableinBQ = true;
                        break;
                    }
                }
                if (isTableinBQ == false) return;
                try
                {

                    var row = new BigQueryInsertRow();
                    var dRow = new Dictionary<string, object>();
                    if (msgtype != "Delete")
                    {
                        string queryBQ = $"SELECT column_name FROM `{projectId}.{datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{entityName}' ORDER BY column_name";
                        var results = client.CreateQueryJob(queryBQ, parameters: null).PollUntilCompleted();
                        List<string> fields = new List<string>();

                        foreach (var col in results.GetQueryResults())
                        {
                            fields.Add(col["column_name"].ToString().ToLower());
                        }

                        _logger.LogInformation($" {fields.Count} column names loaded...");

                        var lattributes = lparsedmsg.InputParameters[0].value.Attributes;

                        _logger.LogInformation($"Entity {entityName} with id {entityID} retrieved...");

                        var entityRecord = GetEntity(entityID, entityName);

                        _logger.LogInformation($"Entity retrieved...");
                        foreach (var attribute in lattributes)
                        {
                            if (fields.Contains(attribute.key.ToString()))
                            {
                                var cKey = attribute.key.ToString();
                                var cValue = GetValueForAttribute(attribute, entityRecord.FormattedValues);

                                if (cValue != null && cKey == entityName + "id")
                                {
                                    dRow.Add(cKey, cValue);
                                    dRow.Add("id", cValue);

                                }
                                else if (cValue != null)
                                {
                                    dRow.Add(cKey, cValue);
                                }
                            }

                        }
                    }
                    string bQuery = "";

                    if (msgtype == "Create")
                    {
                        row.Add(dRow);
                        client.InsertRow(projectId, datasetId, entityName, row, null);
                    }
                    else if (msgtype == "Update")
                    {
                        try
                        {
                            bQuery = $"Update `{projectId}.{datasetId}.{entityName}` set {string.Join(",", dRow.Select(k => $"{k.Key} = '{k.Value}'"))} where {primarykey} = '{entityID}'";

                            BigQueryParameter[] parameters = null;
                            var result = client.ExecuteQuery(bQuery, parameters);

                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("affect rows in the streaming buffer"))
                            {
                                var clonedsbmsg = new ServiceBusMessage(serviceBusMessage)
                                {
                                    ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(minutesToWait)
                                };
                                await sbsender.ScheduleMessageAsync(clonedsbmsg, clonedsbmsg.ScheduledEnqueueTime);
                                _logger.LogInformation($"Successfully scheduled {lparsedmsg.MessageId} in the queue");
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
                            var result = client.ExecuteQuery(bQuery, parameters);
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("affect rows in the streaming buffer"))
                            {
                                var clonedsbmsg = new ServiceBusMessage(serviceBusMessage)
                                {
                                    ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(minutesToWait)
                                };
                                await sbsender.ScheduleMessageAsync(clonedsbmsg, clonedsbmsg.ScheduledEnqueueTime);
                            }
                            else
                            {
                                _logger.LogInformation($"Successfully scheduled {lparsedmsg.MessageId} in the queue");
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
                //try
                //{
                //    if (ex.Message.Contains("Too many DML statements outstanding against table"))
                //    {
                //        throw;
                //    }
                //    else
                //    {
                //        Dead - letter the message
                //    _logger.LogInformation($"Message Id: {message.MessageId}");
                //        _logger.LogInformation($"Lock token: {message.LockToken}");
                //        await sbclient.CreateReceiver("datafromcrm", "subscriptionbq-pr").DeadLetterMessageAsync(message, "Exception", ex.Message);
                //        _logger.LogError($"Message dead-lettered due to exception: {ex.Message}");
                //    }
                //}
                //catch (Exception deadLetterEx)
                //{
                //    _logger.LogCritical($"Failed to dead-letter message: {deadLetterEx.Message}");
                //    throw;
                //}
            }
        }
        private static dynamic GetValueForAttribute(dynamic attribute, FormattedValueCollection coldictionary)
        {
            if (attribute == null || attribute.value == null)
            {
                _logger.LogWarning("Attribute or attribute value is null.");
                return null;
            }
            try
            {
                var emptyJValue = new Newtonsoft.Json.Linq.JValue("");
                if (attribute.value is Newtonsoft.Json.Linq.JValue)
                {

                    if (attribute.value.Value is true || attribute.value.Value is false)
                    {
                        return attribute.value.Value.ToString();
                    }
                    else
                    {
                        if (attribute.value.Value.GetType().FullName == "System.String")
                        {
                            string resultValue = Regex.Replace(attribute.value.Value, @"\t|\n|\r", " ");
                            return resultValue.Replace("'", @"\'");
                        }
                        else if (attribute.value.Value.GetType().FullName == "System.DateTime")
                        {
                            // convert to TimeStamp
                            return attribute.value.Value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
                        }
                        else
                        {
                            return attribute.value.Value;
                        }
                    }
                }
                else
                {
                    if (((string)attribute.value.__type).StartsWith("OptionSetValue"))
                    {
                        string attName = ((Newtonsoft.Json.Linq.JValue)((Newtonsoft.Json.Linq.JProperty)((Newtonsoft.Json.Linq.JContainer)attribute).First).Value).Value.ToString();
                        var osLabel = coldictionary[attribute.key.Value];

                        if (osLabel != null)
                        {
                            return osLabel;
                        }
                        else
                        {
                            return attribute.value.Value.Value;
                        }
                    }
                    else if (((string)attribute.value.__type).StartsWith("EntityReference"))
                    {
                        return attribute.value.Id.Value;
                    }
                    else if (((string)attribute.value.__type).StartsWith("EntityCollection"))
                    {
                        if (attribute.value.Entities.Count > 0)
                        {
                            foreach (var entity in attribute.value.Entities[0].Attributes)
                            {
                                if (entity.key == "partyid")
                                {
                                    return entity.value.Id;
                                }
                            }
                        }

                        return emptyJValue;
                    }
                    else
                    {
                        return attribute.value.Id.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static Entity GetEntity(string entityId, string entityName)
        {
            using (var svc = new CrmServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}"))
            {

                var entId = new Guid(entityId);

                var entity = svc.Retrieve(entityName, entId, new ColumnSet(true));
                _logger.LogInformation($"Entity - {entity.Id} retrieved...");
                return entity;

            }

        }

        
        public static async Task SBMessagePush(dynamic message, dynamic messagesender, int minutesToWait)
        {
            try
            {
                var clonedsbmsg = new ServiceBusMessage(message.Body)
                {
                    ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(minutesToWait)
                };
                await messagesender.ScheduleMessageAsync(clonedsbmsg, clonedsbmsg.ScheduledEnqueueTime);

            }
            catch (Exception ex)
            {
                throw;
            }
        }


    }
}
