using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

public class DlqFanoutTimer
{
    private readonly DlqReprocessor _dlq;
    private readonly ILogger<DlqFanoutTimer> _log;

    // per-queue gate to avoid overlapping DLQ runs
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, SemaphoreSlim> _gates = new();

    public DlqFanoutTimer(DlqReprocessor dlq, ILogger<DlqFanoutTimer> log)
    {
        _dlq = dlq;
        _log = log;
    }

    [Function("dlq-fanout")]
    public async Task Run([TimerTrigger("15 */1 * * * *")] TimerInfo _) // offset by 15s from your main fanout
    {
        var queuesCsv = Environment.GetEnvironmentVariable("Queues") ?? "";
        var queues = queuesCsv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (queues.Length == 0) { _log.LogWarning("No queues configured in 'Queues'"); return; }

        int degree = Math.Min(6, queues.Length); // throttle concurrent DLQ reprocesses
        var throttler = new SemaphoreSlim(degree);
        var tasks = new List<Task>(queues.Length);

        foreach (var q in queues)
        {
            var gate = _gates.GetOrAdd(q, _ => new SemaphoreSlim(1, 1));
            if (!await gate.WaitAsync(0))
            {
                _log.LogWarning("DLQ reprocess for {Queue} already running; skip this tick", q);
                continue;
            }

            await throttler.WaitAsync();
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await _dlq.ReprocessQueueDlqAsync(q, TimeSpan.FromSeconds(2));
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "DLQ reprocess failed for {Queue}", q);
                }
                finally
                {
                    gate.Release();
                    throttler.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
    }
}
