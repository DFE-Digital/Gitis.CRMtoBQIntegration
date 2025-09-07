using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using BQSender.DTO;
using Google; // must contain class ServiceBusBQ { public Dictionary<string, object> Fields { get; set; } }

public class BQProcessor
{
    private readonly BigQueryClient _bq;
    private readonly ReceiverPoolFactory _poolFactory;
    private readonly ILogger<BQProcessor> _log;

    private static string ProjectId => Environment.GetEnvironmentVariable("projectId");
    private static string DatasetId => Environment.GetEnvironmentVariable("datasetId");

    public BQProcessor(BigQueryClient bq, ReceiverPoolFactory poolFactory, ILogger<BQProcessor> log)
    {
        _bq = bq;
        _poolFactory = poolFactory;
        _log = log;
    }

    /// <summary>
    /// Pulls from queue using a receiver pool, batches to BigQuery, then settles (complete/DLQ) in parallel.
    /// </summary>
    //public async Task ProcessQueueAsync(
    //    string queueName,
    //    int poolSize,
    //    int prefetch,
    //    int maxMessagesPerQueue,
    //    TimeSpan receiveWait)
    //{
    //    var receivers = _poolFactory.GetPool(queueName, poolSize, prefetch);
    //    int perReceiver = Math.Max(1, (int)Math.Ceiling((double)maxMessagesPerQueue / receivers.Length));

    //    // 1) Receive in parallel and KEEP which receiver got which message
    //    var receiveTasks = receivers.Select(async r => new { r, msgs = await r.ReceiveMessagesAsync(perReceiver, receiveWait) }).ToArray();
    //    await Task.WhenAll(receiveTasks);
    //    var rcvd = receiveTasks
    //        .SelectMany(t => t.Result.msgs.Select(m => (receiver: t.Result.r, msg: m)))
    //        .ToList();
    //    if (rcvd.Count == 0) { _log.LogInformation("No messages on {Queue}", queueName); return; }

    //    // 2) Pre-validate; DLQ invalid; build rows + mapping
    //    var settled = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    //    var rows = new List<BigQueryInsertRow>(rcvd.Count);
    //    var rowToMsg = new List<(ServiceBusReceiver r, ServiceBusReceivedMessage m)>(rcvd.Count);

    //    foreach (var (r, m) in rcvd)
    //    {
    //        try
    //        {
    //            var payload = JsonConvert.DeserializeObject<ServiceBusBQ>(m.Body.ToString());
    //            if (payload?.Fields == null) throw new InvalidOperationException("Missing Fields");
    //            var row = new BigQueryInsertRow();
    //            foreach (var kv in payload.Fields) row.Add(kv.Key, kv.Value);
    //            rows.Add(row);
    //            rowToMsg.Add((r, m));
    //        }
    //        catch (Exception ex)
    //        {
    //            await r.DeadLetterMessageAsync(m, "Prevalidation failed", ex.Message);
    //            settled.Add(m.LockToken);
    //        }
    //    }
    //    if (rows.Count == 0) { _log.LogWarning("All messages failed prevalidation on {Queue}", queueName); return; }

    //    // 3) Renew locks if we’re close to expiry before calling BigQuery
    //    var now = DateTimeOffset.UtcNow;
    //    var minLockedUntil = rowToMsg.Min(t => t.m.LockedUntil);
    //    if (minLockedUntil - now < TimeSpan.FromSeconds(10))
    //    {
    //        await Task.WhenAll(rowToMsg
    //            .Where(t => !settled.Contains(t.m.LockToken))
    //            .Select(t => t.r.RenewMessageLockAsync(t.m)));
    //    }

    //    try
    //    {
    //        // 4) Insert to BigQuery (micro-batch)
    //        var result = await _bq.InsertRowsAsync(ProjectId, DatasetId, queueName, rows.ToArray(),
    //                                               new InsertOptions { SkipInvalidRows = true });

    //        if (result.Status == BigQueryInsertStatus.AllRowsInserted)
    //        {
    //            await Task.WhenAll(rowToMsg
    //                .Where(t => !settled.Contains(t.m.LockToken))
    //                .Select(t => t.r.CompleteMessageAsync(t.m)));
    //            return;
    //        }

    //        if (result.Status == BigQueryInsertStatus.NoRowsInserted)
    //        {
    //            var reason = "BigQuery: no rows inserted";
    //            var desc = string.Join("; ", result.Errors.SelectMany(e => e).Select(e => $"{e.Reason}:{e.Message}"));
    //            await Task.WhenAll(rowToMsg
    //                .Where(t => !settled.Contains(t.m.LockToken))
    //                .Select(t => t.r.DeadLetterMessageAsync(t.m, reason, desc)));
    //            return;
    //        }

    //        // Partial failure: map OriginalRowIndex -> row/message
    //        var failed = result.Errors.Where(e => e.OriginalRowIndex.HasValue)
    //            .GroupBy(e => e.OriginalRowIndex!.Value)
    //            .ToDictionary(g => g.Key, g => string.Join("; ", g.SelectMany(x => x).Select(x => $"{x.Reason}:{x.Message}")));

    //        var completes = new List<Task>(rowToMsg.Count);
    //        var dlqs = new List<Task>(rowToMsg.Count);
    //        for (int i = 0; i < rowToMsg.Count; i++)
    //        {
    //            var (r, m) = rowToMsg[i];
    //            if (settled.Contains(m.LockToken)) continue;

    //            if (failed.TryGetValue(i, out var err))
    //                dlqs.Add(r.DeadLetterMessageAsync(m, "BigQuery insert failed", err));  // permanent row issue
    //            else
    //                completes.Add(r.CompleteMessageAsync(m));
    //        }
    //        await Task.WhenAll(completes.Concat(dlqs));
    //    }
    //    catch (GoogleApiException gex) when ((int)gex.HttpStatusCode == 429 || (int)gex.HttpStatusCode >= 500)
    //    {
    //        // TRANSIENT BigQuery problem → abandon so they retry later
    //        _log.LogWarning(gex, "Transient BQ error on {Queue}; abandoning {Count}", queueName, rowToMsg.Count);
    //        await Task.WhenAll(rowToMsg
    //            .Where(t => !settled.Contains(t.m.LockToken))
    //            .Select(t => t.r.AbandonMessageAsync(t.m)));
    //        // (Optional) better: schedule to a retry queue with delay to avoid hammering
    //    }
    //    catch (Exception ex)
    //    {
    //        // Unknown/permanent processor error → DLQ to stop poison cycling
    //        _log.LogError(ex, "Processor error on {Queue}; DLQ-ing {Count}", queueName, rowToMsg.Count);
    //        await Task.WhenAll(rowToMsg
    //            .Where(t => !settled.Contains(t.m.LockToken))
    //            .Select(t => t.r.DeadLetterMessageAsync(t.m, "Processor error", ex.Message)));
    //    }
    //}

    public async Task ProcessQueueAsync(string queueName, int poolSize, int prefetch, int maxMessagesPerQueue, TimeSpan receiveWait)
    {
        var receivers = _poolFactory.GetPool(queueName, poolSize, prefetch);
        int perReceiver = Math.Max(1, (int)Math.Ceiling((double)maxMessagesPerQueue / receivers.Length));

        // Receive and KEEP who got what
        var receiveTasks = receivers.Select(async r => new { r, msgs = await r.ReceiveMessagesAsync(perReceiver, receiveWait) }).ToArray();
        await Task.WhenAll(receiveTasks);

        var rcvd = receiveTasks
            .SelectMany(t => t.Result.msgs.Select(m => (receiver: t.Result.r, msg: m)))
            .ToList();

        if (rcvd.Count == 0) return;

        // Build rows + mapping; DLQ invalid immediately
        var settled = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var rows = new List<BigQueryInsertRow>(rcvd.Count);
        var rowToMsg = new List<(ServiceBusReceiver r, ServiceBusReceivedMessage m)>(rcvd.Count);

        foreach (var (r, m) in rcvd)
        {
            try
            {
                var payload = Newtonsoft.Json.JsonConvert.DeserializeObject<BQSender.DTO.ServiceBusBQ>(m.Body.ToString());
                if (payload?.Fields == null) throw new InvalidOperationException("Missing Fields.");
                var row = new BigQueryInsertRow();
                foreach (var kv in payload.Fields) row.Add(kv.Key, kv.Value);
                rows.Add(row);
                rowToMsg.Add((r, m));
            }
            catch (Exception ex)
            {
                await SafeDeadLetterAsync(r, m, "Prevalidation failed", ex.Message);
                settled.Add(m.LockToken);
            }
        }
        if (rows.Count == 0) return;

        // ---- CHUNK + RENEW LOCKS (critical) ----
        const int BQ_BATCH = 250; // tune 150–300 depending on your BQ latency & lock duration
        for (int offset = 0; offset < rows.Count; offset += BQ_BATCH)
        {
            var sliceRows = rows.Skip(offset).Take(BQ_BATCH).ToArray();
            var sliceMap = rowToMsg.Skip(offset).Take(sliceRows.Length).ToList();

            // Renew if any lock is close to expiry (< 15s)
            await RenewIfCloseAsync(sliceMap, settled, thresholdSeconds: 15);

            try
            {
                var result = await _bq.InsertRowsAsync(
                    Environment.GetEnvironmentVariable("projectId"),
                    Environment.GetEnvironmentVariable("datasetId"),
                    queueName,
                    sliceRows,
                    new InsertOptions { SkipInvalidRows = true });

                if (result.Status == BigQueryInsertStatus.AllRowsInserted)
                {
                    await Task.WhenAll(sliceMap
                        .Where(t => !settled.Contains(t.m.LockToken))
                        .Select(t => SafeCompleteAsync(t.r, t.m)));
                }
                else if (result.Status == BigQueryInsertStatus.NoRowsInserted)
                {
                    var desc = string.Join("; ", result.Errors.SelectMany(e => e).Select(e => $"{e.Reason}:{e.Message}"));
                    await Task.WhenAll(sliceMap
                        .Where(t => !settled.Contains(t.m.LockToken))
                        .Select(t => SafeDeadLetterAsync(t.r, t.m, "BigQuery: no rows inserted", desc)));
                }
                else // SomeRowsInserted
                {
                    var failed = result.Errors.Where(e => e.OriginalRowIndex.HasValue)
                        .GroupBy(e => e.OriginalRowIndex!.Value)
                        .ToDictionary(g => g.Key, g => string.Join("; ", g.SelectMany(x => x).Select(x => $"{x.Reason}:{x.Message}")));

                    var completes = new List<Task>(sliceMap.Count);
                    var dlqs = new List<Task>(sliceMap.Count);

                    for (int i = 0; i < sliceMap.Count; i++)
                    {
                        var (r, m) = sliceMap[i];
                        if (settled.Contains(m.LockToken)) continue;

                        if (failed.TryGetValue(i, out var err))
                            dlqs.Add(SafeDeadLetterAsync(r, m, "BigQuery insert failed", err));
                        else
                            completes.Add(SafeCompleteAsync(r, m));
                    }
                    await Task.WhenAll(completes.Concat(dlqs));
                }
            }
            catch (Google.GoogleApiException gex) when ((int)gex.HttpStatusCode == 429 || (int)gex.HttpStatusCode >= 500)
            {
                // TRANSIENT BQ issue -> Abandon (or schedule to retry queue if you have one)
                await Task.WhenAll(sliceMap
                    .Where(t => !settled.Contains(t.m.LockToken))
                    .Select(t => SafeAbandonAsync(t.r, t.m)));
            }
            catch (Exception ex)
            {
                // Unknown processor error -> DLQ to stop poison cycling
                await Task.WhenAll(sliceMap
                    .Where(t => !settled.Contains(t.m.LockToken))
                    .Select(t => SafeDeadLetterAsync(t.r, t.m, "Processor error", ex.Message)));
            }
        }
    }

    // Lock renewal helper
    private static async Task RenewIfCloseAsync(
        IEnumerable<(ServiceBusReceiver r, ServiceBusReceivedMessage m)> items,
        HashSet<string> settled,
        int thresholdSeconds)
    {
        var now = DateTimeOffset.UtcNow;
        var toRenew = items
            .Where(t => !settled.Contains(t.m.LockToken))
            .Where(t => (t.m.LockedUntil - now) < TimeSpan.FromSeconds(thresholdSeconds))
            .Select(t => t.r.RenewMessageLockAsync(t.m));

        await Task.WhenAll(toRenew);
    }

    // Settlement helpers that swallow lock-lost errors (so we don’t rethrow & cause re-delivery)
    private static async Task SafeCompleteAsync(ServiceBusReceiver r, ServiceBusReceivedMessage m)
    {
        try { await r.CompleteMessageAsync(m); }
        catch (Azure.Messaging.ServiceBus.ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessageLockLost || ex.Reason == ServiceBusFailureReason.MessageNotFound)
        { /* ignore */ }
    }

    private static async Task SafeDeadLetterAsync(ServiceBusReceiver r, ServiceBusReceivedMessage m, string reason, string desc)
    {
        try { await r.DeadLetterMessageAsync(m, reason, desc); }
        catch (Azure.Messaging.ServiceBus.ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessageLockLost || ex.Reason == ServiceBusFailureReason.MessageNotFound)
        { /* ignore */ }
    }

    private static async Task SafeAbandonAsync(ServiceBusReceiver r, ServiceBusReceivedMessage m)
    {
        try { await r.AbandonMessageAsync(m); }
        catch (Azure.Messaging.ServiceBus.ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessageLockLost || ex.Reason == ServiceBusFailureReason.MessageNotFound)
        { /* ignore */ }
    }

}
