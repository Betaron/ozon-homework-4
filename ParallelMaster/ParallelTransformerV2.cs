using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using ParallelMaster.Enums;
using ParallelMaster.Extentions;

namespace ParallelMaster;

public class ParallelTransformerV2<TInput, TOutput>
    where TInput : class
    where TOutput : class
{
    /// <summary>
    /// Функция трансформации данных.
    /// </summary>
    public Func<TInput, TOutput> TransformFunction { get; init; }

    /// <summary>
    /// Путь к файлу-источнику входных данных.
    /// </summary>
    public string InputPath { get; init; }

    /// <summary>
    /// Путь к файлу, куда будут сохранены результаты вычислений.
    /// </summary>
    public string OutputPath { get; init; }

    /// <summary>
    /// Размер буфера для хранения прочитанных данны. Максимум входных данных хранящихся в памяти.
    /// </summary>
    public int Buffers { get; init; }

    /// <summary>
    /// Флаг исполнения.
    /// </summary>
    public bool IsExecuting { get; private set; }

    /// <summary>
    /// Логгер записывающий данные процесса исполнения.
    /// </summary>
    public ILogger? Logger { get; set; }

    /// <summary>
    /// Степень параллелизма. Сколько потоков будет одновременно производить расчеты.
    /// </summary>
    public uint ParallelismDegree { get; init; }


    public ParallelTransformerV2(
        Func<TInput, TOutput> transformFunction,
        string inputPath,
        string? outputPath = null,
        uint parallelismDegree = 1,
        int buffers = 1,
        ILogger? logger = null)
    {
        TransformFunction = transformFunction;
        InputPath = inputPath;
        OutputPath = outputPath ?? inputPath.Insert(
            inputPath.LastIndexOf('.'), "_calculated");
        ParallelismDegree = parallelismDegree;
        Buffers = buffers < 1 ? 1 : buffers;
        Logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken stopingToken)
    {
        if (IsExecuting)
        {
            throw new InvalidOperationException(message:
                "Execution already started.");
        }

        IsExecuting = true;

        var startTime = DateTime.UtcNow;

        var inputChannel = Channel.CreateBounded<TInput>(new BoundedChannelOptions(Buffers)
        {
            SingleReader = false,
            SingleWriter = true
        });

        var outputChannel = Channel.CreateBounded<TOutput>(new BoundedChannelOptions(Buffers)
        {
            SingleReader = true,
            SingleWriter = false
        });

        var operationLogger = new OperationLogger(Logger);

        using var CsvWorker = new CsvFileWorker(InputPath, OutputPath);

        var readTask = ReadFileAsync(
            CsvWorker,
            inputChannel.Writer,
            stopingToken,
            operationLogger.LogWriter);

        var tasks = new List<Task>();
        for (int i = 0; i < ParallelismDegree; i++)
        {
            tasks.Add(Transform(
                TransformFunction,
                inputChannel.Reader,
                outputChannel.Writer,
                stopingToken,
                operationLogger.LogWriter));
        }

        var writeTask = WriteFileAsync(
            CsvWorker,
            outputChannel.Reader,
            stopingToken,
            operationLogger.LogWriter);

        var logTask = operationLogger.LogProgressAsync(stopingToken);

        await readTask;

        await Task.WhenAll(tasks);
        outputChannel.Writer.Complete();

        await writeTask;
        operationLogger.LogWriter.Complete();

        var endTime = DateTime.UtcNow;

        await logTask;

        Logger?.LogInformation($"Total time: {(endTime - startTime)}");

        IsExecuting = false;
    }

    private Task ReadFileAsync(
        CsvFileWorker worker,
        ChannelWriter<TInput> inputWriter,
        CancellationToken token,
        ChannelWriter<OperationType>? logWriter = null)
    {
        return Task.Factory.StartNew(async () =>
        {
            var reader = worker.CsvReader;
            reader.Read();
            reader.ReadHeader();

            while (worker.CsvReader.Read())
            {
                token.ThrowIfCancellationRequested();

                var record = reader.GetRecord<TInput>();
                if (record == null)
                {
                    continue;
                }

                await inputWriter.WriteAsync(record);

                if (logWriter is not null)
                {
                    await logWriter.WriteAsync(OperationType.Read);
                }
            }
            inputWriter.Complete();
        }).Unwrap();
    }

    private Task WriteFileAsync(
        CsvFileWorker worker,
        ChannelReader<TOutput> outputReader,
        CancellationToken token,
        ChannelWriter<OperationType>? logWriter = null)
    {
        return Task.Factory.StartNew(async () =>
        {
            var writer = worker.CsvWriter;

            writer.UseSnakeCaseHeaders<TOutput>();

            writer.WriteHeader<TOutput>();
            writer.NextRecord();
            writer.Flush();

            await foreach (var output in outputReader.ReadAllAsync(token))
            {
                writer.WriteRecord(output);
                writer.NextRecord();
                writer.Flush();

                if (logWriter is not null)
                {
                    await logWriter.WriteAsync(OperationType.Write);
                }
            }
        }).Unwrap();
    }

    private Task Transform(
        Func<TInput, TOutput> transform,
        ChannelReader<TInput> inputReader,
        ChannelWriter<TOutput> outputWriter,
        CancellationToken token,
        ChannelWriter<OperationType>? logWriter = null)
    {
        return Task.Factory.StartNew(async () =>
        {
            //await Console.Out.WriteLineAsync(Thread.CurrentThread.Name);
            await foreach (var input in inputReader.ReadAllAsync(token))
            {
                var result = transform.Invoke(input);

                await outputWriter.WriteAsync(result);

                if (logWriter is not null)
                {
                    await logWriter.WriteAsync(OperationType.Calculate);
                }
            }
        }).Unwrap();
    }
}
