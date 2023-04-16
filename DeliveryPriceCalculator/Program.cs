using System.Configuration;
using DeliveryPriceCalculator;
using DeliveryPriceCalculator.models;
using Microsoft.Extensions.Logging;
using ParallelMaster;

internal class Program
{
    private static async Task Main(string[] args)
    {
        string? path;
        do
        {
            Console.Write("Enter Path: ");
            path = Console.ReadLine();
        }
        while (!File.Exists(path));

        ILogger consoleLogger = new FlatConsoleLogger("Delivery Price Calculator");

        ParallelTransformer<GoodParamsModel, GoodPriceModel> pt = new(
            Calculator.Calculate,
            path,
            parallelismDegree: 10,
            inputBufferSize: 400,
            logger: consoleLogger);

        var configtTask = Task.Factory.StartNew(() =>
        {
            while (pt.IsExecuting)
            {
                try
                {
                    ConfigurationManager.RefreshSection("appSettings");
                    Configuration config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

                    var result = uint.Parse(ConfigurationManager.AppSettings["ParallelismDegree"]);

                    if (result != pt.ParallelismDegree)
                    {
                        pt.ParallelismDegree = result;
                    }
                }
                finally { Thread.Sleep(1000); }
            }
        });

        var exe = pt.Execute();
        await configtTask;
        await exe;
    }
}
