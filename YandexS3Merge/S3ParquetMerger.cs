using Amazon.S3;
using Amazon.S3.Model;
using Parquet;
using Parquet.Schema;

namespace YandexS3Merge;

/// <summary>
/// Основной класс для объединения Parquet-файлов из S3 хранилища
/// </summary>
public class S3ParquetMerger : IDisposable
{
    private readonly IAmazonS3 _s3Client;
    private readonly AppConfig _config;
    private readonly string _tempFilePath;
    
    /// <summary>
    /// Конструктор инициализирует S3 клиент и создает временный файл
    /// </summary>
    /// <param name="config">Конфигурация приложения</param>
    public S3ParquetMerger(AppConfig config)
    {
        _config = config;
        _s3Client = S3ClientFactory.CreateClient(config);
        _tempFilePath = Path.GetTempFileName(); // Создаем временный файл для больших данных
        
        // Убедимся, что временный файл будет удален при аварийном завершении
        File.Delete(_tempFilePath); 
    }
    
    /// <summary>
    /// Основной метод выполнения процесса объединения
    /// </summary>
    public async Task MergeFilesAsync()
    {
        try
        {
            // 1. Получаем список Parquet-файлов в бакете
            var files = await ListParquetFilesAsync();
            Console.WriteLine($"Найдено {files.Count} файлов для объединения");
            
            // 2. Проверяем наличие выходного файла
            ValidateFiles(files);
            
            // 3. Читаем схему данных из первого файла
            var schema = await ReadSchemaAsync(files.First());
            
            // 4. Проверяем совместимость схем во всех файлах
            ValidateAllSchemas(files, schema);

            // 5. Объединяем файлы и загружаем результат
            await MergeAndUploadAsync(files, schema);
        }
        finally
        {
            // Гарантированная очистка временных ресурсов
            Cleanup();
        }
    }
    
    /// <summary>
    /// Получает список всех Parquet-файлов в указанном бакете
    /// </summary>
    private async Task<List<S3Object>> ListParquetFilesAsync()
    {
        var request = new ListObjectsV2Request
        {
            BucketName = _config.BucketName,
            Prefix = _config.Prefix // Поиск в определенной папке если необходимо
        };

        var files = new List<S3Object>();
        ListObjectsV2Response response;
        // Обрабатываем пагинацию (S3 возвращает максимум 1000 объектов за раз)
        do
        {
            response = await _s3Client.ListObjectsV2Async(request);
            files.AddRange(response.S3Objects.Where(f => f.Key.EndsWith(".parquet")));
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated == true);

        return files;
    }
    
    /// <summary>
    /// Проверяет, не существует ли уже выходной файл
    /// </summary>
    private void ValidateFiles(List<S3Object> files)
    {
        if (files.Any(f => f.Key.Equals(_config.OutputFile)))
            throw new InvalidOperationException(
                $"Выходной файл {_config.OutputFile} уже существует");
    }
    
    /// <summary>
    /// Читает схему данных из указанного Parquet-файла
    /// </summary>
    private async Task<ParquetSchema> ReadSchemaAsync(S3Object file)
    {
        using var stream = await GetSeekableStreamAsync(file);
        using var reader = await ParquetReader.CreateAsync(stream);
        return reader.Schema;
    }
    
    /// <summary>
    /// Проверяет совпадение схем во всех файлах
    /// </summary>
    private async Task ValidateAllSchemas(List<S3Object> files, ParquetSchema schema)
    {
        foreach (var file in files.Skip(1))
        {
            using var stream = await GetSeekableStreamAsync(file);
            using var reader = await ParquetReader.CreateAsync(stream);
            if (!reader.Schema.Equals(schema))
                throw new InvalidDataException($"Schema mismatch in {file.Key}");
        }
    }
    
    /// <summary>
    /// Основной метод объединения файлов и загрузки результата
    /// </summary>
    private async Task MergeAndUploadAsync(List<S3Object> files, ParquetSchema schema)
    {
        // Используем FileStream для работы с большими файлами
        await using (var fileStream = new FileStream(_tempFilePath, FileMode.Create, FileAccess.Write))
        await using (var writer = await ParquetWriter.CreateAsync(schema, fileStream))
        {
            foreach (var file in files)
            {
                // Читаем данные из каждого файла
                using var stream = await GetSeekableStreamAsync(file);
                using var reader = await ParquetReader.CreateAsync(stream);
                
                // Обрабатываем все группы строк в файле
                for (int i = 0; i < reader.RowGroupCount; i++)
                {
                    using var rowGroup = reader.OpenRowGroupReader(i);
                    
                    // Читаем все колонки асинхронно
                    var columns = await Task.WhenAll(
                        schema.GetDataFields().Select(df => rowGroup.ReadColumnAsync(df))
                    );
                    
                    // Записываем данные в выходной файл
                    using var groupWriter = writer.CreateRowGroup();
                    foreach (var column in columns)
                    {
                        await groupWriter.WriteColumnAsync(column);
                    }
                }
            }
        }

        // Загружаем результат в S3
        await UploadResultAsync();
    }

    /// <summary>
    /// Загружает объединенный файл в S3 хранилище
    /// </summary>
    private async Task UploadResultAsync()
    {
        await using var fileStream = new FileStream(
            _tempFilePath, 
            FileMode.Open,
            FileAccess.Read
        );
        var request = new PutObjectRequest
        {
            BucketName = _config.BucketName,
            Key = _config.OutputFile,
            InputStream = fileStream,
            DisablePayloadSigning = true // Требуется для Yandex Cloud
        };

        await _s3Client.PutObjectAsync(request);
    }

    /// <summary>
    /// Создает seekable поток для чтения файла из S3
    /// </summary>
    private async Task<Stream> GetSeekableStreamAsync(S3Object file)
    {
        var response = await _s3Client.GetObjectAsync(file.BucketName, file.Key);
        var memoryStream = new MemoryStream();
        await response.ResponseStream.CopyToAsync(memoryStream);
        memoryStream.Position = 0; // Сбрасываем позицию для последующего чтения
        return memoryStream;
    }

    /// <summary>
    /// Очищает временные ресурсы
    /// </summary>
    private void Cleanup()
    {
        if (File.Exists(_tempFilePath))
            File.Delete(_tempFilePath);
    }

    /// <summary>
    /// Освобождает ресурсы S3 клиента
    /// </summary>
    public void Dispose()
    {
        _s3Client?.Dispose();
        GC.SuppressFinalize(this);
    }
}