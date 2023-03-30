using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using ParallelMaster.Extentions;

namespace ParallelMaster;
public class ParallelTransformer<TInput, TOutput>
    where TInput : class
    where TOutput : class
{
    private readonly Func<TInput, TOutput> _transformFunction;
    private readonly string _inputPath;
    private readonly string _outputPath;
    private uint _parallelismDegree;

    private readonly object _inputLock = new();
    private readonly object _outputLock = new();

    public uint ParallelismDegree
    {
        get => _parallelismDegree;
        set
        {
            _parallelismDegree = value;
            OnParallelismDegreeChanged();
        }
    }

    public ParallelTransformer(
        Func<TInput, TOutput> transformFunction,
        string inputPath,
        string? outputPath = null,
        uint parallelismDegree = 1)
    {
        _transformFunction = transformFunction;
        _inputPath = inputPath;
        _outputPath = outputPath ?? inputPath.Insert(inputPath.LastIndexOf('.'), "_calculated");
        _parallelismDegree = parallelismDegree;
    }

    private void OnParallelismDegreeChanged()
    {

    }

    public async Task Execute()
    {
        var readConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            PrepareHeaderForMatch = (args) => args.Header.ToLower().Replace("_", ""),
            Delimiter = ", "
        };

        var writeConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = ", ",
            HasHeaderRecord = false,
            Encoding = Encoding.UTF8
        };

        using var sr = new StreamReader(_inputPath);
        using var csvReader = new CsvReader(sr, readConfig);
        using var sw = new StreamWriter(_outputPath, append: false);
        using var csvWriter = new CsvWriter(sw, writeConfig);
        csvReader.Read();
        csvReader.ReadHeader();

        csvWriter.UseSnakeCaseHeaders<TOutput>();

        csvWriter.WriteHeader<TOutput>();
        csvWriter.NextRecord();
        csvWriter.Flush();

        var tasks = new List<Task>()
        {
            GetTransformTask(csvReader, csvWriter, CancellationToken.None),
            GetTransformTask(csvReader, csvWriter, CancellationToken.None),
            GetTransformTask(csvReader, csvWriter, CancellationToken.None)
        };

        await Task.WhenAll(tasks);
    }

    private Task GetTransformTask(
        CsvReader csvReader,
        CsvWriter csvWriter,
        CancellationToken token)
    {
        return Task.Factory.StartNew(() =>
        {
            while (!token.IsCancellationRequested)
            {
                TInput? inputRecord = null;
                lock (_inputLock)
                {
                    if (csvReader.Read())
                    {
                        inputRecord = csvReader.GetRecord<TInput>();
                    }
                }

                if (inputRecord == null) { break; }

                var result = _transformFunction.Invoke(inputRecord);

                lock (_outputLock)
                {
                    csvWriter.WriteRecord(result);
                    csvWriter.NextRecord();
                    csvWriter.Flush();
                }
            }
        }, token);
    }
}
