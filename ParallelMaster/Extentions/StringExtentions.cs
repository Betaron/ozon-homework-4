namespace ParallelMaster.Extentions;
internal static class StringExtentions
{
    public static string CamelToSnakeCase(this string source) =>
        string.Concat(source.Select((character, index) =>
                index > 0 && char.IsUpper(character)
                    ? "_" + character
                    : character.ToString()))
            .ToLower();
}
