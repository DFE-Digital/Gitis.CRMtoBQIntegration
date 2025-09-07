using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using static System.Runtime.InteropServices.JavaScript.JSType;

public class FanoutTimer
{
    private readonly BQProcessor _processor;
    private readonly ILogger<FanoutTimer> _log;
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, System.Threading.SemaphoreSlim> _queueGates = new();

    public FanoutTimer(BQProcessor processor, ILogger<FanoutTimer> log)
    {
        _processor = processor;
        _log = log;
    }

    // Runs every minute
    [Function("fanout")]
    public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo _)
    {
        var queuesCsv = Environment.GetEnvironmentVariable("Queues") ?? "";
        var queues = queuesCsv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (queues.Length == 0)
        {
            _log.LogWarning("No queues configured in 'Queues'. Nothing to process.");
            return;
        }

        // Tunables (or move to app settings)
        int degree = Math.Min(8, queues.Length);                      // max queues processed in parallel
        int poolSize = int.TryParse(Environment.GetEnvironmentVariable("SB_POOL_SIZE"), out var ps) ? Math.Max(1, ps) : 3;
        int prefetch = int.TryParse(Environment.GetEnvironmentVariable("SB_PREFETCH"), out var pf) ? Math.Max(1, pf) : 500;
        int maxPerQ = int.TryParse(Environment.GetEnvironmentVariable("MAX_MESSAGES_PER_QUEUE"), out var mm) ? Math.Max(1, mm) : 1000;
        var recvWait = TimeSpan.FromSeconds(int.TryParse(Environment.GetEnvironmentVariable("RECEIVE_WAIT_SECONDS"), out var rw) ? Math.Max(1, rw) : 2);

        

        //var throttler = new SemaphoreSlim(degree);
        var tasks = new List<Task>(queues.Length);

        foreach (var q in queues)
        {
            //await throttler.WaitAsync();
            //tasks.Add(Task.Run(async () =>
            //{
            //    try
            //    {
            //        await _processor.ProcessQueueAsync(q, poolSize, prefetch, maxPerQ, recvWait);
            //    }
            //    catch (Exception ex)
            //    {
            //        _log.LogError(ex, "Queue {Queue} failed during fanout tick", q);
            //    }
            //    finally
            //    {
            //        throttler.Release();
            //    }
            //}));
            var gate = _queueGates.GetOrAdd(q, _ => new System.Threading.SemaphoreSlim(1, 1));
            if (!await gate.WaitAsync(0))
            {
                _log.LogWarning("Previous run still processing {Queue}; skipping this tick", q);
                continue;
            }

            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await _processor.ProcessQueueAsync(q, poolSize, prefetch, maxPerQ, recvWait);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Queue {Queue} failed during fanout tick", q);
                }
                finally
                {
                    gate.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
    }
}
