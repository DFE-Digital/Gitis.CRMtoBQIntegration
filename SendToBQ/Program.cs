using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SendToBQ
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var host = new HostBuilder()
            .ConfigureFunctionsWebApplication()
            .ConfigureServices(services =>
            {
                services.AddApplicationInsightsTelemetryWorkerService();
                services.ConfigureFunctionsApplicationInsights();
                services.AddSingleton(sp =>
                {
                    var conn = Environment.GetEnvironmentVariable("CrmToBqConnection");
                    return new ServiceBusClient(conn, new ServiceBusClientOptions
                    {
                        TransportType = ServiceBusTransportType.AmqpWebSockets,
                        RetryOptions = new ServiceBusRetryOptions
                        {
                            Mode = ServiceBusRetryMode.Exponential,
                            MaxRetries = 3,
                            TryTimeout = TimeSpan.FromSeconds(12),
                            Delay = TimeSpan.FromSeconds(0.8),
                            MaxDelay = TimeSpan.FromSeconds(5)
                        }
                    });
                });
                services.AddSingleton<ServiceBusFactory>();
                services.AddSingleton(sp =>
                {
                    string projectId = Environment.GetEnvironmentVariable("projectId");

                    var param = new JsonCredentialParameters
                    {
                        Type = Environment.GetEnvironmentVariable("googlecredentials:type"),
                        ProjectId = Environment.GetEnvironmentVariable("googlecredentials:project_id"),
                        PrivateKeyId = Environment.GetEnvironmentVariable("googlecredentials:private_key_id"),
                        PrivateKey = (Environment.GetEnvironmentVariable("googlecredentials:private_key") ?? "").Replace("\\n", "\n"),
                        ClientEmail = Environment.GetEnvironmentVariable("googlecredentials:client_email"),
                        ClientId = Environment.GetEnvironmentVariable("googlecredentials:client_id"),
                        //AuthUri = Environment.GetEnvironmentVariable("googlecredentials:auth_uri"),
                        TokenUri = Environment.GetEnvironmentVariable("googlecredentials:token_uri") ?? "https://oauth2.googleapis.com/token",
                        // provider_x509 & client_x509 are optional for JWT creds
                    };

                    GoogleCredential cred = GoogleCredential.FromJsonParameters(param);
                    return BigQueryClient.Create(projectId, cred);
                });

                services.AddSingleton<Processor.BQProcessor>();

            })
            //.ConfigureLogging(logging => {
            //    logging.Services.Configure<LoggerFilterOptions>(options => {
            //        LoggerFilterRule defaltRule = options.Rules.FirstOrDefault(rule => rule.ProviderName == "Microsoft.Extensions.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider");
            //        if (defaltRule is not null)
            //        {
            //            options.Rules.Remove(defaltRule);
            //        }
            //    });
            //})
            .ConfigureLogging(logging =>
            {
                logging.Services.Configure<LoggerFilterOptions>(opts =>
                {
                    // keep AI provider, but tune noisy namespaces instead
                    opts.AddFilter("Azure.Messaging.ServiceBus", LogLevel.Information);
                    opts.AddFilter("Microsoft.Azure.Amqp", LogLevel.Warning);
                    opts.AddFilter("Google", LogLevel.Information);
                });
            })
            .Build();

            host.Run();
        }
    }
}


