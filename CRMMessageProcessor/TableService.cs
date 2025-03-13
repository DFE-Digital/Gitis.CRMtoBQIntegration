using Azure;
using Azure.Data.Tables;
using System.Collections.Generic;

namespace CRMMessageProcessor
{

    public class TableService
    {
        private readonly TableClient _tableClient;

        public TableService(string connectionString, string tableName)
        {
            _tableClient = new TableClient(connectionString, tableName);
            _tableClient.CreateIfNotExists();
        }

        // CREATE
        public void AddEntity<T>(T entity) where T : class, ITableEntity
        {
            _tableClient.AddEntity(entity);
        }

        // READ (Get Single Entity)
        public T GetEntity<T>(string partitionKey, string rowKey) where T : class, ITableEntity
        {
            try
            {
                return _tableClient.GetEntity<T>(partitionKey, rowKey);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                return null;
            }
        }

        public List<T> GetAllEntities<T>() where T : class, ITableEntity, new()
        {
            List<T> entities = new();
            Pageable<T> results = _tableClient.Query<T>();

            foreach (var entity in results)
            {
                entities.Add(entity);
            }
            return entities;
        }


        // UPDATE
        public void UpdateEntity<T>(T entity) where T : class, ITableEntity
        {
            _tableClient.UpsertEntity<T>(entity, TableUpdateMode.Replace);
        }

        // DELETE
        public void DeleteEntity(string partitionKey, string rowKey)
        {
            _tableClient.DeleteEntity(partitionKey, rowKey);
        }
    }
}
