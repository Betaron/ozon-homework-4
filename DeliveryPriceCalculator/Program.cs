using System.Configuration;
using DeliveryPriceCalculator;
using DeliveryPriceCalculator.models;
using Microsoft.Extensions.Logging;
using ParallelMaster;

string? path;
do
{
    Console.Write("Enter Path: ");
    path = Console.ReadLine();
}
while (!File.Exists(path));

ConfigurationManager.RefreshSection("appSettings");
Configuration config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

var result = uint.Parse(ConfigurationManager.AppSettings["ParallelismDegree"] ?? "1");

ILogger consoleLogger = new FlatConsoleLogger("Delivery Price Calculator");

var pt = new ParallelTransformerV2<GoodParamsModel, GoodPriceModel>(
    Calculator.Calculate,
    path,
    parallelismDegree: result,
    buffers: 128,
    logger: consoleLogger);

await pt.ExecuteAsync(CancellationToken.None);