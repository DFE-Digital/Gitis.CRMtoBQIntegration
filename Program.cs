using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SendCRMChangesToBQ
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
                services.AddSingleton(serviceProvider =>
                {
                    // Retrieve the connection string from environment variables or configuration
                    var connectionString = Environment.GetEnvironmentVariable("sbconnection");

                    // Initialize the ServiceBusClient with custom options if necessary
                    return new ServiceBusClient(connectionString, new ServiceBusClientOptions
                    {
                        TransportType = ServiceBusTransportType.AmqpTcp
                    });
                });
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders(); // Clear default logging providers
                logging.AddConsole(); // Adds console logging
                logging.SetMinimumLevel(LogLevel.Information); // Set the minimum log level
            })
            .Build();

            host.Run();
        }
    }
}


