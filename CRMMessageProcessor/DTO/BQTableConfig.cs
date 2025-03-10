using Azure;
using Azure.Data.Tables;
using System;


namespace CRMMessageProcessor.DTO
{

    public class BQTableConfig : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        // Custom properties
        public string TableName { get; set; }
        public string TableJSON { get; set; }
    }

}
