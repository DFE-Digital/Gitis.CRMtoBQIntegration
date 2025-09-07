using System;
using Azure.Messaging.ServiceBus;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();

        // Service Bus client (singleton). Use AmqpTcp if 5671 is open; else AmqpWebSockets (443).
        services.AddSingleton(sp =>
        {
            var conn = Environment.GetEnvironmentVariable("CrmToBqConnection");
            var useTcp = (Environment.GetEnvironmentVariable("SB_USE_TCP") ?? "false").Equals("true", StringComparison.OrdinalIgnoreCase);
            return new ServiceBusClient(conn, new ServiceBusClientOptions
            {
                TransportType = useTcp ? ServiceBusTransportType.AmqpTcp : ServiceBusTransportType.AmqpWebSockets,
                RetryOptions = new ServiceBusRetryOptions
                {
                    Mode = ServiceBusRetryMode.Exponential,
                    MaxRetries = 5,
                    TryTimeout = TimeSpan.FromSeconds(15),
                    Delay = TimeSpan.FromMilliseconds(400),
                    MaxDelay = TimeSpan.FromSeconds(8)
                }
            });
        });

        // Receiver pool cache (singleton)
        services.AddSingleton<ReceiverPoolFactory>();

        // BigQuery client (singleton). Make sure private_key has real newlines.
        services.AddSingleton(sp =>
        {
            var projectId = Environment.GetEnvironmentVariable("projectId");
            var key = (Environment.GetEnvironmentVariable("googlecredentials:private_key") ?? "").Replace("\\n", "\n");
            var email = Environment.GetEnvironmentVariable("googlecredentials:client_email");
            var token = Environment.GetEnvironmentVariable("googlecredentials:token_uri") ?? "https://oauth2.googleapis.com/token";

            var sa = new ServiceAccountCredential(
                new ServiceAccountCredential.Initializer(email,token)
                .FromPrivateKey(key));

            var cred = GoogleCredential.FromServiceAccountCredential(sa);
            return BigQueryClient.Create(projectId, cred);
        });

        services.AddSingleton<BQProcessor>();
        services.AddSingleton<DlqReprocessor>();
    })
    .Build();

host.Run();
