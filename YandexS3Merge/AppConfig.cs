
using System.Text.Json;

namespace YandexS3Merge;
/// <summary>
/// Класс для хранения конфигурации приложения
/// </summary>
public class AppConfig
{
    /// <summary>Ключ доступа Yandex Cloud</summary>
    public string AccessKey { get; set; }
    /// <summary>Секретный ключ Yandex Cloud</summary>
    public string SecretKey { get; set; }
    /// <summary>Имя бакета в Object Storage</summary>
    public string BucketName { get; set; }
    
    /* Настройки путей */
    public string Prefix { get; set; }               // Префикс для поиска файлов
    public string OutputFile { get; set; }           // Путь выходного файла
    
    /* Настройки подключения S3 */
    public string ServiceURL { get; set; } = "https://storage.yandexcloud.net";
    public string Region { get; set; } = "ru-central1";
    public bool ForcePathStyle { get; set; } = true;
    public bool UseHttp { get; set; } = false;
    public int S3BufferSize { get; set; } = 65536;   // 64 KB
    public int MaxErrorRetry { get; set; } = 3;
    
    
    /// <summary>
    /// Загружает конфигурацию из JSON-файла
    /// </summary>
    /// <param name="configPath">Путь к файлу конфигурации</param>
    public static AppConfig LoadConfig(string configPath)
    {
        if (!File.Exists(configPath))
            throw new FileNotFoundException($"Файл конфигурации {configPath} не найден");

        var json = File.ReadAllText(configPath);
        var config = JsonSerializer.Deserialize<AppConfig>(json);

        return config ?? throw new InvalidDataException("Некорректный формат файла конфигурации");
    }
}