using Parquet;
using Parquet.Schema;

namespace YandexS3Merge;

public class ParquetService
{
    /// <summary>
    /// Читает схему данных из Parquet-потока
    /// </summary>
    /// <param name="stream">Входной поток с Parquet-данными</param>
    /// <returns>Схема данных файла</returns>
    public async Task<ParquetSchema> ReadSchemaAsync(Stream stream)
    {
        using var reader = await ParquetReader.CreateAsync(stream);
        return reader.Schema;
    }

    /// <summary>
    /// Сравнивает схему в потоке с ожидаемой схемой
    /// </summary>
    /// <exception cref="InvalidDataException">Выбрасывается при несовпадении схем</exception>
    public async Task ValidateSchemaAsync(Stream stream, ParquetSchema expectedSchema)
    {
        using var reader = await ParquetReader.CreateAsync(stream);
        if (!reader.Schema.Equals(expectedSchema))
            throw new InvalidDataException("Обнаружено несовпадение схем данных");
    }

    /// <summary>
    /// Объединяет несколько Parquet-файлов в один
    /// </summary>
    /// <param name="getStreams">Коллекция фабрик потоков для чтения</param>
    /// <param name="schema">Общая схема данных</param>
    /// <param name="outputStream">Поток для записи результата</param>
    public async Task MergeFilesAsync(
        IEnumerable<Func<Task<Stream>>> getStreams, 
        ParquetSchema schema, 
        Stream outputStream)
    {
        await using var writer = await ParquetWriter.CreateAsync(schema, outputStream);
        
        foreach (var getStream in getStreams)
        {
            using var stream = await getStream();
            using var reader = await ParquetReader.CreateAsync(stream);
            
            // Обрабатываем все группы строк в файле
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using var rowGroup = reader.OpenRowGroupReader(i);
                
                // Параллельное чтение всех колонок
                var columns = await Task.WhenAll(
                    schema.GetDataFields().Select(df => rowGroup.ReadColumnAsync(df))
                );
                
                // Запись данных в выходной файл
                using var groupWriter = writer.CreateRowGroup();
                foreach (var column in columns)
                {
                    await groupWriter.WriteColumnAsync(column);
                }
            }
        }
    }
}