namespace YandexS3Merge;

class Program
{
    static async Task Main()
    {
        try
        {
            // Загрузка конфигурации из JSON
            var config = AppConfig.LoadConfig("../../../appsettings.json");
            
            Console.WriteLine("Запуск процесса объединения Parquet-файлов...");
            
            // Инициализация и запуск процесса
            using var merger = new S3ParquetMerger(config);
            await merger.MergeFilesAsync();
            
            Console.WriteLine("Объединение успешно завершено");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка: {ex.Message}");
            Environment.Exit(1);
        }
    }
}