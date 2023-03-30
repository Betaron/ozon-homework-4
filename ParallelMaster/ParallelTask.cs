using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using ParallelMaster.Extentions;

namespace ParallelMaster;
public class ParallelTask<TOutput, TInput>
    where TInput : class
    where TOutput : class
{
    public delegate TOutput PTask(TInput input);

    private readonly PTask _task;
    private readonly string _inputPath;
    private readonly string _outputPath;

    public ParallelTask(
        ParallelTask<TOutput, TInput>.PTask task,
        string inputPath,
        string? outputPath = null)
    {
        _task = task;
        _inputPath = inputPath;
        _outputPath = outputPath ?? inputPath.Insert(inputPath.LastIndexOf('.'), "_calculated");
    }

    public async void Execute()
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
        var records = csvReader.GetRecordsAsync<TInput>();

        csvWriter.UseSnakeCaseHeaders<TOutput>();

        csvWriter.WriteHeader<TOutput>();
        csvWriter.NextRecord();
        csvWriter.Flush();

        await foreach (var item in records)
        {
            var res = _task.Invoke(item);

            csvWriter.WriteRecord(res);
            csvWriter.NextRecord();
            csvWriter.Flush();
        }
    }
}
