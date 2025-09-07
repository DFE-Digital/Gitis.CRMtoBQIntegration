using System.Collections.Concurrent;
using System.Linq;
using Azure.Messaging.ServiceBus;

public sealed class ReceiverPoolFactory
{
    private readonly ServiceBusClient _client;
    private readonly ConcurrentDictionary<string, ServiceBusReceiver[]> _pools = new();
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new();

    public ReceiverPoolFactory(ServiceBusClient client) => _client = client;

    public ServiceBusReceiver[] GetPool(string queue, int poolSize, int prefetch, SubQueue subQueue = SubQueue.None)
    {
        var key = $"{queue}|{poolSize}|{prefetch}|{(int)subQueue}";
        return _pools.GetOrAdd(key, _ =>
        {
            return Enumerable.Range(0, poolSize)
                .Select(_ => _client.CreateReceiver(queue, new ServiceBusReceiverOptions
                {
                    PrefetchCount = prefetch,
                    SubQueue = subQueue
                }))
                .ToArray();
        });
    }

    public ServiceBusSender GetSender(string queue) =>
        _senders.GetOrAdd(queue, q => _client.CreateSender(q));
}
