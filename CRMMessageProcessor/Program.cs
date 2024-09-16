using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CRMMessageProcessor
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
                //services.AddSingleton(serviceProvider =>
                //{
                //    var connectionString = Environment.GetEnvironmentVariable("sbconnection");

                //    return new ServiceBusClient(connectionString, new ServiceBusClientOptions
                //    {
                //        TransportType = ServiceBusTransportType.AmqpTcp
                //    });
                //});
            })
            .ConfigureLogging(logging => {
                logging.Services.Configure<LoggerFilterOptions>(options => {
                    LoggerFilterRule defaltRule = options.Rules.FirstOrDefault(rule => rule.ProviderName == "Microsoft.Extensions.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider");
                    if (defaltRule is not null)
                    {
                        options.Rules.Remove(defaltRule);
                    }
                });
            })
            .Build();

            host.Run();
        }
    }
}


