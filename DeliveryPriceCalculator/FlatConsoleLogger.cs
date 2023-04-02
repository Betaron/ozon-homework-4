using Microsoft.Extensions.Logging;

namespace DeliveryPriceCalculator;
internal class FlatConsoleLogger : ILogger
{
    private readonly string _name;
    private ulong _count;

    public FlatConsoleLogger(string name)
    {
        _name = name;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => default!;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        ConsoleColor originalColor = Console.ForegroundColor;
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.Write($"{_count}");
        Console.ForegroundColor = ConsoleColor.DarkGray;
        Console.Write($" - [ {_name} ]\t");
        Console.ForegroundColor = originalColor;
        Console.WriteLine($"{formatter(state, exception)}");

        _count++;
    }
}
