using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Parquet;
using Parquet.Schema;

class S3ParquetMerger
{
    private static IAmazonS3 _s3Client;
    
    // ЗАМЕНИТЬ НА СВОИ
    private const string AccessKey = "";
    private const string SecretKey = "";
    private const string BucketName = "test00";
    private const string InputFolder = "";
    private const string OutputFile = "output/combined.parquet";

    static async Task Main()
    {
        try
        {
            InitializeS3Client();
            
            Console.WriteLine("Starting Parquet files merge...");
            var files = await ListParquetFiles();
            Console.WriteLine($"Found {files.Count} files to merge");
            
            ValidateFiles(files);
            var schema = await ReadSchema(files.First());
            ValidateAllSchemas(files, schema);
            
            await using var mergedStream = await MergeFiles(files, schema);
            await UploadResult(mergedStream);
            
            Console.WriteLine($"Successfully merged {files.Count} files into {OutputFile}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            Environment.Exit(1);
        }
    }

    private static void InitializeS3Client()
    {
        var credentials = new BasicAWSCredentials(AccessKey, SecretKey);
        
        _s3Client = new AmazonS3Client(
            credentials,
            new AmazonS3Config
            {
                ServiceURL = "https://storage.yandexcloud.net",
                AuthenticationRegion = "ru-central1",
                ForcePathStyle = true,  // Обязательно для Yandex Cloud
                UseHttp = false,
                BufferSize = 65536,
                MaxErrorRetry = 3
            }
        );
    }

    private static async Task<List<S3Object>> ListParquetFiles()
    {
        var request = new ListObjectsV2Request
        {
            BucketName = BucketName,
            Prefix = InputFolder
        };

        var files = new List<S3Object>();
        ListObjectsV2Response response;
        
        do
        {
            response = await _s3Client.ListObjectsV2Async(request);
            files.AddRange(response.S3Objects.Where(f => f.Key.EndsWith(".parquet")));
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated == true);

        if (files.Count == 0) throw new FileNotFoundException("No Parquet files found");
        return files;
    }

    private static void ValidateFiles(List<S3Object> files)
    {
        if (files.Any(f => f.Key.Equals(OutputFile)))
            throw new InvalidOperationException("Output file already exists");
    }

    private static async Task<ParquetSchema> ReadSchema(S3Object file)
    {
        using var stream = await GetSeekableStream(file);
        using var reader = await ParquetReader.CreateAsync(stream);
        return reader.Schema;
    }

    private static async Task ValidateAllSchemas(List<S3Object> files, ParquetSchema schema)
    {
        foreach (var file in files.Skip(1))
        {
            using var stream = await GetSeekableStream(file);
            using var reader = await ParquetReader.CreateAsync(stream);
            if (!reader.Schema.Equals(schema))
                throw new InvalidDataException($"Schema mismatch in {file.Key}");
        }
    }

    private static async Task<MemoryStream> MergeFiles(List<S3Object> files, ParquetSchema schema)
    {
        var outputStream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(schema, outputStream))
        {
            foreach (var file in files)
            {
                using var stream = await GetSeekableStream(file);
                using var reader = await ParquetReader.CreateAsync(stream);
                
                for (int i = 0; i < reader.RowGroupCount; i++)
                {
                    using var rowGroup = reader.OpenRowGroupReader(i);
                    var columns = await Task.WhenAll(
                        schema.GetDataFields().Select(df => rowGroup.ReadColumnAsync(df))
                    );
                    
                    using var groupWriter = writer.CreateRowGroup();
                    foreach (var column in columns)
                    {
                        await groupWriter.WriteColumnAsync(column);
                    }
                }
            }
        }
        
        outputStream.Position = 0;
        return outputStream;
    }

    private static async Task UploadResult(MemoryStream stream)
    {
        var request = new PutObjectRequest
        {
            BucketName = BucketName,
            Key = OutputFile,
            InputStream = stream,
            AutoCloseStream = false,
            DisablePayloadSigning = true
        };

        await _s3Client.PutObjectAsync(request);
    }

    private static async Task<MemoryStream> GetSeekableStream(S3Object file)
    {
        using var response = await _s3Client.GetObjectAsync(file.BucketName, file.Key);
        var memoryStream = new MemoryStream();
        await response.ResponseStream.CopyToAsync(memoryStream);
        memoryStream.Position = 0;
        return memoryStream;
    }
}