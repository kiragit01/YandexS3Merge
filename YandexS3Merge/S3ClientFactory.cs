using Amazon.Runtime;
using Amazon.S3;

namespace YandexS3Merge;
/// <summary>
/// Фабрика для создания настроенного клиента Amazon S3
/// </summary>
public static class S3ClientFactory
{
    /// <summary>
    /// Создает и настраивает клиент для работы с Yandex Cloud S3
    /// </summary>
    /// <param name="config">Конфигурация приложения</param>
    /// <returns>Настроенный экземпляр S3 клиента</returns>
    public static IAmazonS3 CreateClient(AppConfig config)
    {
        // Используем Basic аутентификацию с ключами из конфига
        var credentials = new BasicAWSCredentials(config.AccessKey, config.SecretKey);
        
        return new AmazonS3Client(
            credentials,
            new AmazonS3Config
            {
                ServiceURL = "https://storage.yandexcloud.net",
                AuthenticationRegion = "ru-central1", // Регион размещения бакета
                ForcePathStyle = true,  // Обязательно для Yandex Cloud S3
                UseHttp = false,         // Используем HTTPS
                BufferSize = 65536,      // Размер буфера для операций
                MaxErrorRetry = 3        // Количество повторов при ошибках
            }
        );
    }
}