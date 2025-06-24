using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace CRMMessageProcessor
{
    public class CRMBQRecordsCounter
    {
        private static readonly string projectId = Environment.GetEnvironmentVariable("projectId");
        private static readonly string datasetId = Environment.GetEnvironmentVariable("datasetId");
        private static readonly string d365Environment = Environment.GetEnvironmentVariable("d365Environment");       
        private static readonly string clientid = Environment.GetEnvironmentVariable("clientId");
        private static readonly string clientsecret = Environment.GetEnvironmentVariable("clientSecret");
        private static readonly string gtype = Environment.GetEnvironmentVariable("googlecredentials:type");
        private static readonly string gproject_id = Environment.GetEnvironmentVariable("googlecredentials:project_id");
        private static readonly string gprivate_key_id = Environment.GetEnvironmentVariable("googlecredentials:private_key_id");
        private static readonly string gprivate_key = Environment.GetEnvironmentVariable("googlecredentials:private_key");
        private static readonly string gclient_email = Environment.GetEnvironmentVariable("googlecredentials:client_email");
        private static readonly string gclient_id = Environment.GetEnvironmentVariable("googlecredentials:client_id");
        //private static readonly string gauth_uri = Environment.GetEnvironmentVariable("googlecredentials:auth_uri");
        private static readonly string gtoken_uri = Environment.GetEnvironmentVariable("googlecredentials:token_uri");
        //private static readonly string gauth_provider_x509_cert_url = Environment.GetEnvironmentVariable("googlecredentials:auth_provider_x509_cert_url");
        //private static readonly string gclient_x509_cert_url = Environment.GetEnvironmentVariable("googlecredentials:client_x509_cert_url");
        private static readonly string entities = Environment.GetEnvironmentVariable("tablenames");
        //private static readonly string tableStorageString = Environment.GetEnvironmentVariable("tableStorageString");
        private readonly BigQueryClient _bigQueryClient;
        private readonly ILogger<CRMBQRecordsCounter> _logger;
        //private readonly TableService _tableService;

        /*--BQ Table specs---*/
        private static readonly string bqAnalyticsTable = Environment.GetEnvironmentVariable("bqAnalyticsTable");
        private static readonly string bqAnalyticsDataset = Environment.GetEnvironmentVariable("bqAnalyticsDataset");

        public CRMBQRecordsCounter(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<CRMBQRecordsCounter>();

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
            //_tableService = new(tableStorageString, "crmbqtablerecordcount");
        }

        [Function("CRMBQRecordsCounter")]
        public async Task Run([TimerTrigger("0 15 0 * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"Start: {DateTime.Now}");

            try
            {
                var startDateTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day - 1, 0, 0, 0);
                var endDateTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day - 1, 23, 59, 59);

                var tables = entities.Split(",");

                foreach (var table in tables)
                {                    
                    var crm_count = GetTotalCountInGitis(table, startDateTime, endDateTime);

                    string queryBQ = $"SELECT\r\n  COUNT(*) AS count\r\nFROM (\r\n  SELECT\r\n    DISTINCT id,\r\n    MAX(modifiedon) OVER (PARTITION BY id) AS max_timestamp,\r\n  FROM\r\n    `{projectId}.{datasetId}.{table}` )\r\nWHERE\r\n  DATETIME(max_timestamp) >= DATETIME({startDateTime.Year}, {startDateTime.Month}, {startDateTime.Day}, {startDateTime.Hour}, {startDateTime.Minute}, 0)\r\n  AND DATETIME(max_timestamp) <= DATETIME({endDateTime.Year}, {endDateTime.Month}, {endDateTime.Day}, {endDateTime.Hour}, {endDateTime.Minute}, 59)";

                    _logger.LogInformation($"{queryBQ}");
                    var results = await _bigQueryClient.CreateQueryJobAsync(queryBQ, parameters: null);
                    var bqCount = "0";
                    if (results.GetQueryResults().TotalRows > 0)
                    {
                        bqCount = results.GetQueryResults().Select(x => x["count"].ToString()).First();
                    }

                    //_tableService.AddEntity(new DTO.CRMBQMetric
                    //{
                    //    PartitionKey = table,
                    //    RowKey = Guid.NewGuid().ToString(),
                    //    BQ_Count = bqCount,
                    //    CRM_Count = crm_count.ToString(),
                    //    Start_Time = startDateTime,
                    //    End_Time = endDateTime
                    //});


                    BigQueryInsertRow row = new BigQueryInsertRow();

                    row.Add("entity_name", table);
                    row.Add("crm_count", crm_count);
                    row.Add("bq_count", int.Parse(bqCount));
                    row.Add("date_to", endDateTime);
                    row.Add("date_from", startDateTime);
                    row.Add("count_difference", crm_count - int.Parse(bqCount));                 

                    var bigQueryInsertResult = await _bigQueryClient.InsertRowAsync(projectId, bqAnalyticsDataset, bqAnalyticsTable, row);


                }
                if (myTimer.ScheduleStatus is not null)
                {
                    _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
                }
            }
            catch (Exception ex)
            {

                _logger.LogCritical($"Failed contact timer : {ex.Message}");
                throw;

            }
        }

        private int GetTotalCountInGitis(string entity, DateTime start, DateTime end)
        {
            using var svc = new ServiceClient($@"AuthType=ClientSecret;Url={d365Environment};ClientId={clientid};ClientSecret={clientsecret}");

            // Set Condition Values
            var query_createdon_1 = start.ToString("s");
            var query_createdon_2 = end.ToString("s");            

            int pageNumber = 1;
            int fetchCount = 5000;
            int totalRecords = 0;

            QueryExpression query = new QueryExpression(entity)
            {
                ColumnSet = new ColumnSet(false),
                PageInfo = new PagingInfo
                {
                    PageNumber = pageNumber,
                    Count = fetchCount
                }
            };
            query.Criteria.AddCondition("modifiedon", ConditionOperator.Between, query_createdon_1, query_createdon_2);

            EntityCollection results;

            do
            {
                results = svc.RetrieveMultiple(query);
                totalRecords += results.Entities.Count;

                if (results.MoreRecords)
                {
                    query.PageInfo.PageNumber++;
                    query.PageInfo.PagingCookie = results.PagingCookie;
                }
            } while (results.MoreRecords);

            return totalRecords;

        }


    }
}





