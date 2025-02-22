using Amazon.S3;
using Amazon.S3.Model;

namespace YandexS3Merge;

public class S3Service : IDisposable
{
    // Основной клиент для работы с S3 API
    private readonly IAmazonS3 _s3Client;
    
    // Конфигурация приложения с параметрами подключения
    private readonly AppConfig _config;

    public S3Service(AppConfig config)
    {
        _config = config;
        // Инициализация клиента через фабрику
        _s3Client = S3ClientFactory.CreateClient(config);
    }

    /// <summary>
    /// Получает список .parquet файлов из указанного бакета и префикса
    /// </summary>
    /// <returns>Список объектов S3 с метаданными файлов</returns>
    public async Task<List<S3Object>> ListParquetFilesAsync()
    {
        // Используем пагинацию для обработки большого количества файлов
        var request = new ListObjectsV2Request
        {
            BucketName = _config.BucketName,
            Prefix = _config.Prefix
        };

        var files = new List<S3Object>();
        ListObjectsV2Response response;
        do
        {
            response = await _s3Client.ListObjectsV2Async(request);
            files.AddRange(response.S3Objects.Where(f => f.Key.EndsWith(".parquet")));
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated == true); // Обрабатываем все страницы результатов

        return files;
    }

    /// <summary>
    /// Загружает файл из S3 в seekable поток памяти
    /// </summary>
    /// <param name="file">S3 объект для загрузки</param>
    /// <returns>Поток с данными файла</returns>
    public async Task<Stream> GetFileStreamAsync(S3Object file)
    {
        // Загружаем объект и копируем в MemoryStream для поддержки seek
        var response = await _s3Client.GetObjectAsync(file.BucketName, file.Key);
        var memoryStream = new MemoryStream();
        await response.ResponseStream.CopyToAsync(memoryStream);
        memoryStream.Position = 0; // Сброс позиции для последующего чтения
        return memoryStream;
    }
    
    public async Task UploadFileAsync(string localFilePath, string s3Key)
    {
        await using var fileStream = new FileStream(localFilePath, FileMode.Open);
        var request = new PutObjectRequest
        {
            BucketName = _config.BucketName,
            Key = s3Key,
            InputStream = fileStream,
            DisablePayloadSigning = true
        };
        await _s3Client.PutObjectAsync(request);
    }

    /// <summary>
    /// Проверяет существование файла в S3 бакете
    /// </summary>
    /// <param name="s3Key">Полный путь к файлу в S3</param>
    /// <returns>True если файл существует</returns>
    public async Task<bool> FileExistsAsync(string s3Key)
    {
        try
        {
            // Попытка получить метаданные файла
            await _s3Client.GetObjectMetadataAsync(_config.BucketName, s3Key);
            return true;
        }
        catch
        {
            // Если файл не найден, возвращаем false
            return false;
        }
    }

    public void Dispose() => _s3Client?.Dispose();
}