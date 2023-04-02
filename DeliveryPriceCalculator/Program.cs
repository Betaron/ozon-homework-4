using DeliveryPriceCalculator;
using DeliveryPriceCalculator.models;
using Microsoft.Extensions.Logging;
using ParallelMaster;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var path = Path.Combine(Directory.GetCurrentDirectory(), "input.csv");

        ILogger consoleLogger = new FlatConsoleLogger("Delivery Price Calculator");

        ParallelTransformer<GoodParamsModel, GoodPriceModel> pt = new(
            Calculator.Calculate,
            path,
            parallelismDegree: 10,
            inputBufferSize: 20,
            logger: consoleLogger);

        await pt.Execute();
    }
}
