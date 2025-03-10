using Azure;
using Azure.Data.Tables;
using System;


namespace CRMMessageProcessor.DTO
{

    public class CRMBQMetric : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        // Custom properties
        public string BQ_Count { get; set; }
        public string CRM_Count { get; set; }
        public DateTimeOffset? Start_Time { get; set; }
        public DateTimeOffset? End_Time { get; set; }
    }

}
