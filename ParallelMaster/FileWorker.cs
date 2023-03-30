using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;

namespace ParallelMaster;
internal class FileWorker : IDisposable
{
    private readonly string _inputPath;
    private readonly string _outputPath;

    private readonly StreamReader _reader;
    private readonly StreamWriter _writer;

    public CsvReader CsvReader { get; private set; }
    public CsvWriter CsvWriter { get; private set; }

    private readonly CsvConfiguration _readConfig = new(CultureInfo.InvariantCulture)
    {
        Delimiter = ", ",
        PrepareHeaderForMatch = (args) => args.Header.ToLower().Replace("_", "")
    };

    private readonly CsvConfiguration _writeConfig = new(CultureInfo.InvariantCulture)
    {
        Delimiter = ", ",
        HasHeaderRecord = false,
        Encoding = Encoding.UTF8
    };

    public FileWorker(string inputPath, string outputPath)
    {
        _inputPath = inputPath;
        _outputPath = outputPath;

        _reader = new(_inputPath);
        _writer = new(_outputPath);

        CsvReader = new(_reader, _readConfig);
        CsvWriter = new(_writer, _writeConfig);
    }

    public void Dispose()
    {
        _reader.Dispose();
        _writer.Dispose();
        CsvReader.Dispose();
        CsvWriter.Dispose();
    }
}
