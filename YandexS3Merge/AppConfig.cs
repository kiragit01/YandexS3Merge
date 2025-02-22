
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
    /// <summary>Путь к папке с файлами</summary>
    public string Prefix { get; set; }
    /// <summary>Путь для сохранения объединенного файла</summary>
    public string OutputFile { get; set; }
    
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