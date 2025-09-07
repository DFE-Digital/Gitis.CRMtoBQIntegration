using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;

public sealed class ServiceBusFactory
{
    private readonly ServiceBusClient _client;
    private readonly ConcurrentDictionary<string, ServiceBusReceiver> _receivers = new();
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new();

    public ServiceBusFactory(ServiceBusClient client) => _client = client;

    public ServiceBusReceiver GetReceiver(string queue) =>
        _receivers.GetOrAdd(queue, q =>
            _client.CreateReceiver(q, new ServiceBusReceiverOptions { PrefetchCount = 100 }));

    public ServiceBusSender GetSender(string queue) =>
        _senders.GetOrAdd(queue, q => _client.CreateSender(q));
}
