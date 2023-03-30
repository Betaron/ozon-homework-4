using DeliveryPriceCalculator;
using DeliveryPriceCalculator.models;
using ParallelMaster;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var path = Path.Combine(Directory.GetCurrentDirectory(), "input.csv");

        ParallelTransformer<GoodParamsModel, GoodPriceModel> pt = new(
            Calculator.Calculate,
            path);

        await pt.Execute();
    }
}
