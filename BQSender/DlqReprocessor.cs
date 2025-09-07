using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

public sealed class DlqReprocessor
{
    private readonly ReceiverPoolFactory _factory;
    private readonly ILogger<DlqReprocessor> _log;

    // tunables (or move to app settings)
    private const int PoolSize = 2;
    private const int Prefetch = 200;
    private const int MaxDlqRetries = 5;              // max times we’ll pull a DLQ’d item for retry
    private const int MaxPerQueuePerTick = 500;       // cap to avoid huge bursts

    public DlqReprocessor(ReceiverPoolFactory factory, ILogger<DlqReprocessor> log)
    {
        _factory = factory;
        _log = log;
    }

    public async Task ReprocessQueueDlqAsync(string queueName, TimeSpan receiveWait)
    {
        var dlqReceivers = _factory.GetPool(queueName, PoolSize, Prefetch, SubQueue.DeadLetter);
        var sender = _factory.GetSender(queueName);
        var poisonSender = _factory.GetSender($"{queueName}-poison"); // optional quarantine queue

        int perReceiver = Math.Max(1, (int)Math.Ceiling((double)MaxPerQueuePerTick / dlqReceivers.Length));
        var pulls = dlqReceivers.Select(async r => new { r, msgs = await r.ReceiveMessagesAsync(perReceiver, receiveWait) }).ToArray();
        await Task.WhenAll(pulls);

        var dlq = pulls.SelectMany(t => t.Result.msgs.Select(m => (receiver: t.Result.r, msg: m))).ToList();
        if (dlq.Count == 0)
        {
            _log.LogInformation("No DLQ messages on {Queue}", queueName);
            return;
        }

        _log.LogInformation("Pulled {Count} from {Queue} DLQ", dlq.Count, queueName);

        // Process each DLQ message
        var tasks = new List<Task>(dlq.Count);

        foreach (var (r, m) in dlq)
        {
            tasks.Add(HandleDlqMessageAsync(queueName, r, m, sender, poisonSender));
        }

        await Task.WhenAll(tasks);
    }

    private async Task HandleDlqMessageAsync(
        string queue,
        ServiceBusReceiver dlqReceiver,
        ServiceBusReceivedMessage dlqMsg,
        ServiceBusSender activeSender,
        ServiceBusSender poisonSender)
    {
        // Read DLQ metadata (stored by Service Bus in application properties)
        dlqMsg.ApplicationProperties.TryGetValue("DeadLetterReason", out var reasonObj);
        dlqMsg.ApplicationProperties.TryGetValue("DeadLetterErrorDescription", out var descObj);
        string reason = reasonObj?.ToString() ?? "";
        string desc = descObj?.ToString() ?? "";

        // Our own retry count for DLQ reprocessing
        int retry = 0;
        if (dlqMsg.ApplicationProperties.TryGetValue("dlq-retry-count", out var rc) && rc is int rci) retry = rci;

        // Heuristics: decide if this is permanent vs transient
        bool permanent =
            reason.Contains("Prevalidation", StringComparison.OrdinalIgnoreCase) ||
            reason.Contains("BigQuery insert failed", StringComparison.OrdinalIgnoreCase) && IsSchemaOrDataError(desc) ||
            reason.Contains("Processor error", StringComparison.OrdinalIgnoreCase) && retry >= 1; // repeated processing bug

        if (permanent || retry >= MaxDlqRetries)
        {
            // Quarantine: forward to <queue>-poison with context, then COMPLETE the DLQ message
            var qMsg = CloneForResubmit(dlqMsg, keepMessageId: true); // keep ID for trace
            qMsg.ApplicationProperties["quarantine-source"] = "dlq";
            qMsg.ApplicationProperties["quarantine-reason"] = reason;
            qMsg.ApplicationProperties["quarantine-desc"] = desc;

            await poisonSender.SendMessageAsync(qMsg);
            await SafeCompleteAsync(dlqReceiver, dlqMsg);
            return;
        }

        // Transient or unknown → resubmit to active queue with exponential backoff
        var backoffSeconds = (int)Math.Min(900, Math.Pow(2, retry) * 30); // 30s, 60s, 120s, ... up to 15m
        var reMsg = CloneForResubmit(dlqMsg, keepMessageId: false);       // new ID to avoid dedup
        reMsg.ApplicationProperties["dlq-retry-count"] = retry + 1;
        reMsg.ApplicationProperties["dlq-original-seq"] = dlqMsg.SequenceNumber;
        reMsg.ApplicationProperties["dlq-original-enqueue"] = dlqMsg.EnqueuedTime.UtcDateTime.ToString("o");

        // Delay the reprocessing to give external systems time to recover
        var scheduleTime = DateTimeOffset.UtcNow.AddSeconds(backoffSeconds);
        await activeSender.ScheduleMessageAsync(reMsg, scheduleTime);

        // Remove from DLQ only after resubmit succeeds
        await SafeCompleteAsync(dlqReceiver, dlqMsg);
    }

    private static bool IsSchemaOrDataError(string desc)
    {
        // Rough BigQuery permanent error hints; tune as needed for your messages
        if (string.IsNullOrEmpty(desc)) return false;
        var d = desc.ToLowerInvariant();
        return d.Contains("invalid") || d.Contains("schema") || d.Contains("no such field") ||
               d.Contains("type mismatch") || d.Contains("invalid value") || d.Contains("not found: table");
    }

    private static ServiceBusMessage CloneForResubmit(ServiceBusReceivedMessage src, bool keepMessageId)
    {
        var m = new ServiceBusMessage(src.Body)
        {
            ContentType = src.ContentType,
            Subject = src.Subject,
            CorrelationId = src.CorrelationId,
            MessageId = keepMessageId ? src.MessageId : Guid.NewGuid().ToString("N"),
            ApplicationProperties = { } // set below
        };

        foreach (var kv in src.ApplicationProperties)
        {
            // Keep original context but strip SB system DLQ keys to avoid confusion
            if (kv.Key is "DeadLetterReason" or "DeadLetterErrorDescription" or "DeadLetterSource") continue;
            m.ApplicationProperties[kv.Key] = kv.Value;
        }

        // Carry original id for trace even if we replaced MessageId
        m.ApplicationProperties["original-message-id"] = src.MessageId;

        return m;
    }

    private static async Task SafeCompleteAsync(ServiceBusReceiver r, ServiceBusReceivedMessage m)
    {
        try { await r.CompleteMessageAsync(m); }
        catch (ServiceBusException ex) when (ex.Reason is ServiceBusFailureReason.MessageLockLost or ServiceBusFailureReason.MessageNotFound) { }
    }
}
