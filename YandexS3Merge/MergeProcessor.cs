using Amazon.S3.Model;

namespace YandexS3Merge;

public class MergeProcessor : IDisposable
{
    // Сервисы для разделения ответственности
    private readonly S3Service _s3Service;
    private readonly ParquetService _parquetService;
    
    // Конфигурация приложения
    private readonly AppConfig _config;
    
    // Временный файл для промежуточного хранения данных
    private readonly string _tempFilePath;

    public MergeProcessor(AppConfig config)
    {
        _config = config;
        _s3Service = new S3Service(config);
        _parquetService = new ParquetService();
        
        // Создаем и сразу удаляем временный файл для гарантии уникальности
        _tempFilePath = Path.GetTempFileName();
        File.Delete(_tempFilePath); // Фактическое создание будет при записи
    }

    /// <summary>
    /// Основной метод выполнения процесса объединения
    /// </summary>
    public async Task MergeFilesAsync()
    {
        var files = await _s3Service.ListParquetFilesAsync();
        ValidateFileCount(files);
        
        // Проверка конфликтов выходного файла
        if (await _s3Service.FileExistsAsync(_config.OutputFile))
            throw new InvalidOperationException($"Выходной файл {_config.OutputFile} уже существует");

        await ValidateSchemasAsync(files);
        await MergeAndUploadAsync(files);
    }

    /// <summary>
    /// Проверяет наличие файлов для обработки
    /// </summary>
    private void ValidateFileCount(List<S3Object> files)
    {
        if (files.Count == 0)
            throw new InvalidOperationException("Не найдено файлов для объединения");
    }

    /// <summary>
    /// Проверяет совместимость схем всех файлов
    /// </summary>
    private async Task ValidateSchemasAsync(List<S3Object> files)
    {
        // Читаем схему из первого файла
        using var firstStream = await _s3Service.GetFileStreamAsync(files[0]);
        var schema = await _parquetService.ReadSchemaAsync(firstStream);

        // Проверяем все остальные файлы
        foreach (var file in files.Skip(1))
        {
            using var stream = await _s3Service.GetFileStreamAsync(file);
            await _parquetService.ValidateSchemaAsync(stream, schema);
        }
    }

    /// <summary>
    /// Основная логика объединения и загрузки результата
    /// </summary>
    private async Task MergeAndUploadAsync(List<S3Object> files)
    {
        // Создаем фабрики потоков для отложенного чтения
        var getStreams = files.Select(file => 
            new Func<Task<Stream>>(async () => await _s3Service.GetFileStreamAsync(file)));
        
        await using (var outputStream = new FileStream(_tempFilePath, FileMode.Create))
        {
            // Читаем схему из первого файла
            using var firstStream = await _s3Service.GetFileStreamAsync(files[0]);
            var schema = await _parquetService.ReadSchemaAsync(firstStream);
            
            // Выполняем объединение во временный файл
            await _parquetService.MergeFilesAsync(getStreams, schema, outputStream);
        }

        // Загружаем результат в S3
        await _s3Service.UploadFileAsync(_tempFilePath, _config.OutputFile);
        Cleanup();
    }

    /// <summary>
    /// Очищает временные ресурсы
    /// </summary>
    private void Cleanup()
    {
        if (File.Exists(_tempFilePath))
            File.Delete(_tempFilePath);
    }

    public void Dispose() => _s3Service?.Dispose();
}