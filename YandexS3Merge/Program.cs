namespace YandexS3Merge;

class Program
{
    static async Task Main()
    {
        try
        {
            // Инициализация конфигурации из файла
            var config = AppConfig.LoadConfig("../../../appsettings.json");
            Console.WriteLine("Запуск процесса объединения Parquet-файлов...");
            
            // Создание и запуск основного процессора объединения
            using var merger = new MergeProcessor(config);
            await merger.MergeFilesAsync();
            
            Console.WriteLine("Объединение успешно завершено");
        }
        catch (Exception ex)
        {
            // Обработка критических ошибок с выводом в консоль
            Console.WriteLine($"Критическая ошибка: {ex.Message}");
            Environment.Exit(1);
        }
    }
}