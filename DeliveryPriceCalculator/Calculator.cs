using DeliveryPriceCalculator.models;

namespace DeliveryPriceCalculator;
internal class Calculator
{
    private const decimal VolumeToPriceRatio = 4.27m;
    private const decimal WeightToPriceRatio = 4.34m;

    public static GoodPriceModel Calculate(GoodParamsModel good)
    {
        var volumePrice = CalculatePriceByVolume(good);
        var weightPrice = CalculatePriceByWeight(good);

        var resultPrice = Math.Max(volumePrice, weightPrice);

        return new(good.Id, resultPrice);
    }

    private static decimal CalculatePriceByVolume(GoodParamsModel good)
    {
        var volume = good.Height * good.Width * good.Length / 1000m;

        return volume * VolumeToPriceRatio;
    }

    private static decimal CalculatePriceByWeight(GoodParamsModel goods)
    {
        var weight = goods.Weight / 1000m;

        return weight * WeightToPriceRatio;
    }

}
