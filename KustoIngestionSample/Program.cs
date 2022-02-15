using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Azure.Storage.Blobs;
using System.IO;
using Azure.Storage.Blobs.Models;
using System.Threading.Tasks;
using System.Diagnostics;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Blob;

namespace KustoIngestionSample
{
    class Program
    {
        static string StorageConnectionString;
        static string BlobContainerName;
        static string LocalFilePath;

        static string ADXclusterNameAndRegion;
        static string AADAppId;
        static string AADAppSecret;
        static string AADTenent;
        static string ADXDatabaseName;
        static string ADXTableName;
        static string ADXTableMappingName;

        //default ingestion parameters
        static string IngestionType = "Streaming";
        static int IngestionTotalItems = 50000;
        static int IngestionItemsPerBatch = 10000;
        static int IngestionDelayMSPerBatch = 1000; //1 sec
        static bool QueueIngestionFlushImmediately = false;
        static bool DeleteSourceOnSuccessQueueIngestion = true;
        static int AccumulatedItems = 0;

        public static async Task Main(string[] args)
        {
            string connectionStringEngine = String.Empty, connectionStringDM = String.Empty;

            //Reading application settings
            try
            {
                IConfigurationRoot configuration = new ConfigurationBuilder()
                        .AddJsonFile("appSettings.json")
                        .Build();

                StorageConnectionString = configuration["StorageConnectionString"];
                BlobContainerName = configuration["BlobContainerName"];
                LocalFilePath = configuration["LocalFilePath"];

                ADXclusterNameAndRegion = configuration["ADXclusterNameAndRegion"];
                AADAppId = configuration["AADAppId"];
                AADAppSecret = configuration["AADAppSecret"];
                AADTenent = configuration["AADTenent"];
                ADXDatabaseName = configuration["ADXDatabaseName"];
                ADXTableName = configuration["ADXTableName"];
                ADXTableMappingName = configuration["ADXTableMappingName"];

                IngestionType = configuration["IngestionType"];
                IngestionTotalItems = Int32.Parse(configuration["IngestionTotalItems"]);
                IngestionItemsPerBatch = Int32.Parse(configuration["IngestionItemsPerBatch"]);
                IngestionDelayMSPerBatch = Int32.Parse(configuration["IngestionDelayMSPerBatch"]);
                QueueIngestionFlushImmediately = bool.Parse(configuration["QueueIngestionFlushImmediately"]);
                DeleteSourceOnSuccessQueueIngestion = bool.Parse(configuration["DeleteSourceOnSuccessQueueIngestion"]);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, reading application settings, failed: {ex.Message}");
            }
            finally
            {
                //Create Kusto connection string with App Authentication
                switch (IngestionType.ToLower())
                {
                    case "queue":
                    case "streaming":
                        connectionStringDM = String.Format($"https://ingest-{ADXclusterNameAndRegion}.kusto.windows.net");
                        connectionStringEngine = String.Format($"https://{ADXclusterNameAndRegion}.kusto.windows.net");
                        break;
                    default:
                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, IngestionType must be Queue or Streaming, press ENTER to exit ...");
                        Console.ReadLine();
                        Environment.Exit(0);                        
                        break;                        
                }
            }

            //Create local file folder if not exists
            String containerName = BlobContainerName;
            String FilePath = LocalFilePath + containerName + @"\";
            try
            {
                if (!System.IO.Directory.Exists(FilePath))
                {
                    System.IO.Directory.CreateDirectory(FilePath);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, create local file folder, failed: {ex.Message}");
                Console.ReadLine();
                Environment.Exit(0);
            }

            //Create Azure Blob Storage container if not exists
            BlobContainerClient blobContainerClient = new BlobContainerClient(StorageConnectionString, containerName);
            try
            {
                await blobContainerClient.CreateIfNotExistsAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, create Azure Blob Storage container, failed: {ex.Message}");
                Console.ReadLine();
                Environment.Exit(0);

            }            

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Restart();

            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, [{IngestionType}] ingestion demo start");
            String BatchTimestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            String BatchId = Guid.NewGuid().ToString();

            int i = 0;
            while (AccumulatedItems < IngestionTotalItems)
            {
                Console.WriteLine();
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Ingestion round {++i}, start");
                int miniIngestionItemsPerBatch = ((AccumulatedItems + IngestionItemsPerBatch) > IngestionTotalItems ? IngestionTotalItems - AccumulatedItems : IngestionItemsPerBatch);

                String miniBatchId = i.ToString().PadLeft(10, '0');
                miniBatchId = String.Format($"{IngestionType.Substring(0, 1).ToLower()}{miniBatchId.Substring(miniBatchId.Length - 10, 10)}");
                String FileName = String.Format($"{BatchTimestamp}-{BatchId}-{miniBatchId}.json");
                
                try
                {
                    List<Item> itemslist = GetItemsToList(miniIngestionItemsPerBatch, BatchId, miniBatchId);
                    await SaveItemsToLocalFilePath(itemslist, FilePath, FileName);

                    if (IngestionType.ToLower() == "queue")
                    {
                        await SaveItemsToBlobContainer(FilePath, FileName, blobContainerClient);
                        BlobClient blob = blobContainerClient.GetBlobClient(FileName);
                        Uri blobSASUri;
                        if (DeleteSourceOnSuccessQueueIngestion)
                            blobSASUri = blob.GenerateSasUri(Azure.Storage.Sas.BlobSasPermissions.Read | Azure.Storage.Sas.BlobSasPermissions.Delete, DateTime.UtcNow.AddHours(4));
                        else
                            blobSASUri = blob.GenerateSasUri(Azure.Storage.Sas.BlobSasPermissions.Read, DateTime.UtcNow.AddHours(4));

                        #region Setup Kusto Queue Ingestion client and properties
                        var kustoConnectionStringBuilderDM =
                            new KustoConnectionStringBuilder(connectionStringDM).WithAadApplicationKeyAuthentication(
                                applicationClientId: AADAppId,
                                applicationKey: AADAppSecret,
                                authority: AADTenent);

                        var KustoQueueclient = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);
                        var kustoQueueIngestionProperties = new KustoQueuedIngestionProperties(databaseName: ADXDatabaseName, tableName: ADXTableName)
                        {
                            ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                            ReportMethod = IngestionReportMethod.QueueAndTable,
                            FlushImmediately = QueueIngestionFlushImmediately,
                            Format = DataSourceFormat.json,
                            IngestionMapping = new IngestionMapping { IngestionMappingKind = Kusto.Data.Ingestion.IngestionMappingKind.Json, IngestionMappingReference = ADXTableMappingName }
                        };
                        var sourceOptions = new StorageSourceOptions() { DeleteSourceOnSuccess = DeleteSourceOnSuccessQueueIngestion };
                        IRetryPolicy retryPolicy = new NoRetry();
                        ((IKustoQueuedIngestClient)KustoQueueclient).QueuePostRequestOptions.RetryPolicy = retryPolicy;
                        #endregion

                        await ExecuteQueueIngestion(blobSASUri, KustoQueueclient, kustoQueueIngestionProperties, sourceOptions);
                    }
                    else if (IngestionType.ToLower() == "streaming")
                    {
                        #region Setup Kusto Streaming Ingestion client and properties
                        var kustoConnectionStringBuilderEngine =
                            new KustoConnectionStringBuilder(connectionStringEngine).WithAadApplicationKeyAuthentication(
                                applicationClientId: AADAppId,
                                applicationKey: AADAppSecret,
                                authority: AADTenent);

                        var KustoStreamingClient = KustoIngestFactory.CreateStreamingIngestClient(kustoConnectionStringBuilderEngine);
                        var kustoStreamingIngestionProperties = new KustoIngestionProperties(databaseName: ADXDatabaseName, tableName: ADXTableName)
                        {
                            Format = DataSourceFormat.json,
                            IngestionMapping = new IngestionMapping { IngestionMappingKind = Kusto.Data.Ingestion.IngestionMappingKind.Json, IngestionMappingReference = ADXTableMappingName }
                        };
                        #endregion

                        await ExecuteStreamingIngestion(FilePath, FileName, KustoStreamingClient, kustoStreamingIngestionProperties);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ingestion round {i}, failed: {ex.Message}");
                }
                finally
                {
                    AccumulatedItems += miniIngestionItemsPerBatch;

                    await CleanUpLocalFilePath(FilePath, FileName);
                    if (!DeleteSourceOnSuccessQueueIngestion)
                    {
                        //await CleanUpBlobContainer(blobContainerClient);
                    }

                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ingestion round {i}, finished (AccumulatedItems={AccumulatedItems}).");
                }
                Thread.Sleep(IngestionDelayMSPerBatch);
            }

            stopwatch.Stop();
            Console.WriteLine();
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, [{IngestionType}] ingestion demo finished");

            switch (IngestionType.ToLower())
            {
                case "queue":
                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, > target Kusto cluster: {connectionStringDM}");
                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, > QueueIngestionFlushImmediately: {QueueIngestionFlushImmediately}");
                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, > DeleteSourceOnSuccessQueueIngestion: {DeleteSourceOnSuccessQueueIngestion}");
                    break;
                case "streaming":
                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, > target Kusto cluster: {connectionStringEngine}");
                    break;
            }            
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, > target database name: {ADXDatabaseName}");
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, > target table name: {ADXTableName}");
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, > target ingestion mapping name: {ADXTableMappingName}");
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ingested {IngestionTotalItems} items through {i} batches in {stopwatch.Elapsed}");
        }

        private static async Task ExecuteQueueIngestion(Uri blobSASUri, IKustoQueuedIngestClient KustoQueueclient, KustoQueuedIngestionProperties kustoQueueIngestionProperties, StorageSourceOptions sourceOptions)
        {
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteQueueIngestion, job start");

            try
            {
                IKustoIngestionResult KustoIngestionresult;
                KustoIngestionresult = KustoQueueclient.IngestFromStorageAsync(uri: blobSASUri.AbsoluteUri, ingestionProperties: kustoQueueIngestionProperties, sourceOptions).Result;

                var IngestResults = KustoIngestionresult.GetIngestionStatusCollection();
                int Pending = 0, Failed = 0, Succeeded = 0;
                foreach (IngestionStatus result in IngestResults)
                {
                    switch (result.Status.ToString())
                    {
                        case "Pending":
                            Pending++;
                            break;
                        case "Failed":
                            Failed++;
                            break;
                        case "Succeeded":
                            Succeeded++;
                            break;
                    }
                }                
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteQueueIngestion, job submitted: {IngestResults.Count()} ({Pending} Pending; {Failed} Failed; {Succeeded} Succeeded; IngestionSourceId:{sourceOptions.SourceId})");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteQueueIngestion, job failed: {ex.Message}");
            }
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteQueueIngestion, job finished");
        }
        
        private static async Task ExecuteStreamingIngestion(String FilePath, String FileName, IKustoIngestClient KustoQueueclient, KustoIngestionProperties kustoQueueIngestionProperties)
        {
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteStreamingIngestion, job start");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Restart();
            long fileSize = 0;
            try
            {
                IKustoIngestionResult KustoIngestionresult;
                using (FileStream fileStream = File.OpenRead(FilePath + FileName))
                {
                    fileSize = fileStream.Length;
                    KustoIngestionresult = KustoQueueclient.IngestFromStreamAsync(stream: fileStream, ingestionProperties: kustoQueueIngestionProperties).GetAwaiter().GetResult();
                }

                var IngestResults = KustoIngestionresult.GetIngestionStatusCollection();
                int Pending = 0, Failed = 0, Succeeded = 0;
                foreach (IngestionStatus result in IngestResults)
                {
                    switch (result.Status.ToString())
                    {
                        case "Pending":
                            Pending++;
                            break;
                        case "Failed":
                            Failed++;
                            break;
                        case "Succeeded":
                            Succeeded++;
                            break;
                    }
                }
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteStreamingIngestion, job submitted: {IngestResults.Count()} ({Pending} Pending; {Failed} Failed; {Succeeded} Succeeded)");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteStreamingIngestion, job failed: {ex.Message}");
            }
            finally
            {
                stopwatch.Stop();
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteStreamingIngestion, job finished: {(fileSize / 1024.0 / 1024.0).ToString("f2")} Mb in {stopwatch.Elapsed}. " +
                $"({(fileSize / 1024.0 / 1024.0 / (stopwatch.ElapsedMilliseconds / 1000)).ToString("f2")} Mb/s).");
            }
        }

        private static async Task SaveItemsToLocalFilePath(List<Item> itemslist, String FilePath, String FileName)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Restart();
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, SaveItemsToLocalFilePath, saving {itemslist.Count} items to local file: {FilePath}{FileName}");

            using (System.IO.StreamWriter file =
                new System.IO.StreamWriter(FilePath + FileName, true))
            {
                foreach (Item item in itemslist)
                {
                    file.WriteLine(JsonConvert.SerializeObject(item).ToString());
                }                
            }            
            stopwatch.Stop();

            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, SaveItemsToLocalFilePath, {itemslist.Count} items are saved in {stopwatch.Elapsed}.");
        }

        private static async Task CleanUpLocalFilePath(String FilePath, String FileName)
        {
            try
            {
                if (System.IO.File.Exists(FilePath + FileName))
                {
                    System.IO.File.Delete(FilePath + FileName);
                }
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpLocalFilePath, {FilePath + FileName}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpLocalFilePath, failed: {ex.Message}");
            }
            finally
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpLocalFilePath, finished");
            }
        }

        private static async Task SaveItemsToBlobContainer(String FilePath, String FileName, BlobContainerClient blobContainerClient)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Restart();
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, SaveItemsToBlobContainer, uploading local file to blob container: {blobContainerClient.Uri}");
            
            BlobClient blob = blobContainerClient.GetBlobClient(FileName);
            long fileSize = 0;
            using (FileStream fileStream = File.OpenRead(FilePath + FileName))
            {
                fileSize = fileStream.Length;
                await blob.UploadAsync(fileStream);
            }
            stopwatch.Stop();

            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, SaveItemsToBlobContainer, file uploaded: {(fileSize / 1024.0 / 1024.0).ToString("f2")} Mb in {stopwatch.Elapsed}. " +
                $"({(fileSize / 1024.0 / 1024.0 / (stopwatch.ElapsedMilliseconds / 1000)).ToString("f2")} Mb/s).");
        }

        private static async Task CleanUpBlobContainer(BlobContainerClient blobContainerClient)
        {
            /*
                Kusto Ingestion (queue) is an aync process, we should manually delete blobs after confirming ingestion completed.
                Batches are sealed when the first condition is met:
                1. The total size of the batched data reaches the size set by the IngestionBatching policy.
                2. The maximum delay time is reached
                3. The number of blobs set by the IngestionBatching policy is reached
                Ref: https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/batchingpolicy
            */

            int segmentSize = 100;
            int i = 1;
            try
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpBlobContainer, start");
                var resultSegment = blobContainerClient.GetBlobsAsync().AsPages(default, segmentSize);
                await foreach (Azure.Page<BlobItem> blobPage in resultSegment)
                {
                    foreach (BlobItem blobItem in blobPage.Values)
                    {
                        await blobContainerClient.DeleteBlobIfExistsAsync(blobItem.Name);
                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpBlobContainer, {i++}, {blobItem.Name}");
                    }
                    Thread.Sleep(1000);
                }
            }
            catch (Azure.RequestFailedException ex)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpBlobContainer, failed: {ex.Message}");
            }
            finally
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpBlobContainer, finished");
            }
        }

        private static List<Item> GetItemsToList(int ItemCount, String BatchId, String miniBatchId)
        {
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, GetItemsToList, generating {ItemCount} items (BatchId = {BatchId}).");
            
            var SampleTags = new Bogus.Faker<tags>()
                .StrictMode(true)
                .RuleFor(o => o.colors, f => f.Commerce.Color())
                .RuleFor(o => o.material, f => f.Commerce.ProductMaterial());

            String BatchIds = String.Format($"{BatchId};{miniBatchId}");
            int Serial = 1;
            var SampleItems = new Bogus.Faker<Item>()
                .StrictMode(false)
                .RuleFor(o => o.ItemBatch, f => BatchIds)
                .RuleFor(o => o.ItemSerial, f => Serial++)
                .RuleFor(o => o.id, f => Guid.NewGuid().ToString())
                .RuleFor(o => o.pk, (f, o) => f.Commerce.Product())
                .RuleFor(o => o.product_id, f => f.Commerce.Ean13())
                .RuleFor(o => o.product_name, f => f.Commerce.Product())
                .RuleFor(o => o.product_category, f => f.Commerce.ProductName())
                .RuleFor(o => o.product_quantity, f => f.Random.Int(1, 500))
                .RuleFor(o => o.product_price, f => Math.Round(f.Random.Double(1, 100), 3))
                .RuleFor(o => o.product_tags, f => SampleTags.Generate(f.Random.Number(1, 5)))
                .RuleFor(o => o.sale_department, f => f.Commerce.Department())
                .RuleFor(o => o.user_mail, f => f.Internet.ExampleEmail())
                .RuleFor(o => o.user_name, f => f.Internet.UserName())
                .RuleFor(o => o.user_country, f => f.Address.CountryCode())
                .RuleFor(o => o.user_ip, f => f.Internet.Ip())
                .RuleFor(o => o.user_avatar, f => f.Internet.Avatar().Replace("cloudflare-ipfs.com/ipfs", "example.net"))
                .RuleFor(o => o.user_comments, f => f.Lorem.Text())
                .RuleFor(o => o.user_isvip, f => f.Random.Bool())
                .RuleFor(o => o.user_login_date, f => f.Date.Recent().ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'ffffff'Z'"))
                .RuleFor(o => o.timestamp, f => DateTime.UtcNow)
                .Generate(ItemCount);

            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, GetItemsToList, {ItemCount} items are generated.");
            return SampleItems.ToList();
        }

    }

}
