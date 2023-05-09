using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;

namespace ParallelMaster.Extentions;
internal static class CsvWriterExtentions
{
    public static void UseSnakeCaseHeaders<T>(this CsvWriter csvWriter)
    {
        var map = new DefaultClassMap<T>();

        map.AutoMap(CultureInfo.InvariantCulture);

        foreach (var memberMap in map.MemberMaps)
        {
            memberMap.Data.Names.Add(memberMap.Data.Member.Name.CamelToSnakeCase());
        }

        csvWriter.Context.RegisterClassMap(map);
    }
}
