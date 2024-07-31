using System;
using System.Collections.Generic;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using static System.Net.Mime.MediaTypeNames;
using System.Text.RegularExpressions;
using Azure.Messaging.ServiceBus;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Crm.Sdk.Messages;
using System.Runtime.Remoting.Contexts;
using System.Windows.Controls.Primitives;

namespace CRMChangestoBQIntegration
{
    public class SBMessageProcess
    {
        //private readonly ILogger _logger;
        private static readonly string projectId = Environment.GetEnvironmentVariable("projectId");
        private static readonly string datasetId = Environment.GetEnvironmentVariable("datasetId");
        private static readonly string prodenvironment = Environment.GetEnvironmentVariable("prodEnvironment");
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
        private static ServiceBusSender sbsender;
        private static ServiceBusClient sbclient;
        private static readonly string sbconnection = Environment.GetEnvironmentVariable("sbconnection");
        private static readonly string sbtopicname = Environment.GetEnvironmentVariable("sbtopicname");

        //public SBMessageProcess(ILoggerFactory loggerFactory)
        //{
        //    _logger = loggerFactory.CreateLogger<SBMessageProcess>();
        //}

        [Function("SBMessageProcess")]
        public static async Task Run([ServiceBusTrigger("datafromcrm", "subscriptionbq-pr", Connection = "sbconnection")] string SbMsg, FunctionContext context)
        {
            var _logger = context.GetLogger(nameof(SBMessageProcess));
            _logger.LogInformation($"C# ServiceBus topic trigger function processed message: {SbMsg}");
            dynamic lparsedmsg = JsonConvert.DeserializeObject(SbMsg);

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
                PrivateKey = gprivate_key, //privateKey.Replace("\\n", "\n"),
                ClientEmail = gclient_email,
                ClientId = gclient_id,
                TokenUrl = gtoken_uri
            };
            var googlecredentials = GoogleCredential.FromJsonParameters(param);
            BigQueryClient client = BigQueryClient.Create(projectId, googlecredentials);
            string queryBQ = $"SELECT column_name FROM `{projectId}.{datasetId}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{entityName}' ORDER BY column_name";
            BigQueryResults results = client.ExecuteQuery(queryBQ, parameters: null);
            List<string> fields = new List<string>();
            foreach (var col in results)
            {
                fields.Add(col["column_name"].ToString().ToLower());
            }
            List<BigQueryTable> tables = client.ListTables(datasetId).ToList();
            bool isTableinBQ = false;
            foreach (BigQueryTable table in tables)
            {
                if (table.ToString().Contains(entityName))
                {
                    isTableinBQ = true;
                    break; }
            }
            if (isTableinBQ == false) return;
            //End Connect Big Query

            //Start Connect CRM
            var svc = new CrmServiceClient($@"AuthType=ClientSecret;url={prodenvironment};ClientId={clientid};ClientSecret={clientsecret}");

            Entity entityRecord = new Entity();
            var optionsetcols = new Dictionary<String, object>();

            try
            {
                //using (var svc = new CrmServiceClient(constr))
                //{
                    if ((svc.IsReady || svc != null) && (msgtype != "Delete"))
                    {
                        Guid entId = new Guid(entityID);
                        if (msgtype != "Delete")
                        {
                            entityRecord = svc.Retrieve(entityName, entId, new ColumnSet(true));
                            foreach (var attribute in entityRecord.Attributes)
                            {
                                //if (((KeyValuePair<string, object>)attribute).Value.ToString()== "Microsoft.Xrm.Sdk.OptionSetValue")
                                if (attribute.Value != null && attribute.Value.ToString().Contains("OptionSetValue"))
                                {
                                    var ckey = ((KeyValuePair<string, object>)attribute).Key;
                                    var cvalue = ((Microsoft.Xrm.Sdk.OptionSetValue)((KeyValuePair<string, object>)attribute).Value).Value;
                                    var clabel = entityRecord.FormattedValues[ckey];
                                    optionsetcols.Add(ckey,clabel);
                                }
                            }
                        }

                    }
                //}
                //End Connect CRM

                dynamic lattributes = lparsedmsg.InputParameters[0].value.Attributes;
                var row = new BigQueryInsertRow();
                var drow = new Dictionary<String, Object>();
                string bQuery = "";

                if (msgtype != "Delete")
                {
                    foreach (dynamic attribute in lattributes)
                    {
                        if (fields.Contains(attribute.key.ToString()))
                        {
                            var cKey = attribute.key.ToString();
                            var cValue = GetValueForAttribute(attribute, entityName, msgtype, optionsetcols);
                            if (cValue != null && cKey == primarykey)
                            {
                                drow.Add(cKey, cValue);
                                drow.Add("id", cValue);
                            }
                            else if (cValue != null)
                            {
                                drow.Add(cKey, cValue);
                            }
                        }
                    }
                }
                if (msgtype == "Create")
                {
                    row.Add(drow);
                    client.InsertRow(projectId, datasetId, entityName, row, null);
                    //sQuery = $"insert into dbo.crm_{entityName} (Id,{string.Join(",", sqlDictionary.Keys.Select(k => $"[{k}]"))}) values (@Id,{string.Join(",", sqlDictionary.Keys.Select(k => $"@{k}"))})";
                }
                else if (msgtype == "Update")
                {
                    try
                    {
                        bQuery = $"Update `{projectId}.{datasetId}.{entityName}` set {string.Join(",", drow.Select(k => $"{k.Key} = '{k.Value}'"))} where {primarykey} = '{entityID}'";
                        BigQueryParameter[] parameters = null;
                        BigQueryResults result = client.ExecuteQuery(bQuery, parameters);

                    }
                    catch (Exception ex)
                    {
                        if (ex.Message.Contains("affect rows in the streaming buffer"))
                        {
                            int minutesToWait = 90; //set the waiting time that service bus holds message
                            sbclient = new ServiceBusClient(sbconnection);
                            sbsender = sbclient.CreateSender(sbtopicname);
                            ServiceBusMessage sbm = new ServiceBusMessage();
                            var clonedsbmsg = new ServiceBusMessage(SbMsg);
                            clonedsbmsg.ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(minutesToWait);
                            await sbsender.ScheduleMessageAsync(clonedsbmsg, clonedsbmsg.ScheduledEnqueueTime);
                            _logger.LogInformation($"Successfully scheduled {lparsedmsg.MessageId} in the queue");
                        }
                        else
                        {
                            _logger.LogCritical($"ServiceBus topic trigger function - See message :- {ex.Message}");
                        }
                    }
                }
                else if (msgtype == "Delete")
                {
                    try { 
                    bQuery = $"Delete from `{projectId}.{datasetId}.{entityName}` where Id = '{entityID}'";
                    BigQueryParameter[] parameters = null;
                    BigQueryResults result = client.ExecuteQuery(bQuery, parameters);
                    }
                    catch (Exception ex)
                    {
                        int minutesToWait = 90; //set the waiting time that service bus holds message
                        sbclient = new ServiceBusClient(sbconnection);
                        sbsender = sbclient.CreateSender(sbtopicname);
                        ServiceBusMessage sbm = new ServiceBusMessage();
                        var clonedsbmsg = new ServiceBusMessage(SbMsg);
                        clonedsbmsg.ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(minutesToWait);
                        await sbsender.ScheduleMessageAsync(clonedsbmsg, clonedsbmsg.ScheduledEnqueueTime);
                        _logger.LogInformation($"Successfully scheduled {lparsedmsg.MessageId} in the queue");
                        _logger.LogCritical($"ServiceBus topic trigger function - See message :- {ex.Message}");
                    }
                }

            }
            catch (JsonSerializationException e)
            {
                _logger.LogCritical($"Entity Name: {entityName}");
                _logger.LogCritical(e.ToString());
                throw e;
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"Entity Name: {entityName}");
                _logger.LogCritical(ex.ToString());
                throw ex;
            }
        }
        private static dynamic GetValueForAttribute(dynamic attribute, dynamic entityname, string msgType, Dictionary<String, object> coldictionary)
        {
            try {
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
                        object osLabel = null;
                        foreach (var element in coldictionary)
                        {
                            if (String.Equals(element.Key, attName))
                            {
                                osLabel = element.Value;
                                break;
                            }
                        }
                        //if (coldictionary.TryGetValue(attName, out osLabel))
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
            catch(Exception ex) {
                return null;
            }
        }
        public static async Task SBMessagePush(dynamic message, dynamic messagesender,int minutesToWait)
        {
            try
            {

                var clonedsbmsg = new ServiceBusMessage(message.Body);
                clonedsbmsg.ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(minutesToWait);
                await messagesender.ScheduleMessageAsync(clonedsbmsg, clonedsbmsg.ScheduledEnqueueTime);
                //log.LogInformation($"Successfully scheduled {message.MessageId} in the queue");

            }
            catch (Exception ex)
            {
                //log.LogCritical($"ServiceBus topic trigger function - See error message :- {exception.Message}");
                throw;
            }
        }
    }
}
