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
        static string storageConnectionString;
        static string BlobContainerName;
        static string LocalFilePath;

        static string ADXclusterNameAndRegion;
        static string AADAppId;
        static string AADAppSecret;
        static string AADTenent;
        static string ADXDatabaseName;
        static string ADXTableName;
        static string ADXTableMappingName;
        static bool IngestionFlushImmediately = false;
        static bool DeleteSourceOnSuccessIngestion = true;

        static int TotalItems = 50000;
        static int AccumulatedItems = 0;
        static int ItemsPerBatch = 10000;
        static int IngestionDelayMS = 1000;
        public static async Task Main(string[] args)
        {
            //Reading settings
            IConfigurationRoot configuration = new ConfigurationBuilder()
                    .AddJsonFile("appSettings.json")
                    .Build();

            storageConnectionString = configuration["StorageConnectionString"];
            BlobContainerName = configuration["BlobContainerName"];
            LocalFilePath = configuration["LocalFilePath"];

            ADXclusterNameAndRegion = configuration["ADXclusterNameAndRegion"];
            AADAppId = configuration["AADAppId"];
            AADAppSecret = configuration["AADAppSecret"];
            AADTenent = configuration["AADTenent"];

            ADXDatabaseName = configuration["ADXDatabaseName"];
            ADXTableName = configuration["ADXTableName"];
            ADXTableMappingName = configuration["ADXTableMappingName"];
            IngestionFlushImmediately = bool.Parse(configuration["IngestionFlushImmediately"]);
            DeleteSourceOnSuccessIngestion = bool.Parse(configuration["DeleteSourceOnSuccessIngestion"]);

            TotalItems = Int32.Parse(configuration["TotalItems"]);
            ItemsPerBatch = Int32.Parse(configuration["ItemsPerBatch"]);
            IngestionDelayMS = Int32.Parse(configuration["IngestionDelayMS"]);

            //Create Kusto connection string with App Authentication
            var kustoConnectionStringBuilderDM =
                new KustoConnectionStringBuilder($"https://ingest-{ADXclusterNameAndRegion}.kusto.windows.net").WithAadApplicationKeyAuthentication(
                    applicationClientId: AADAppId,
                    applicationKey: AADAppSecret,
                    authority: AADTenent);
            
            //Setup Kusto Ingestion
            IKustoQueuedIngestClient Kustoclient = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);
            var kustoIngestionProperties = new KustoQueuedIngestionProperties(databaseName: ADXDatabaseName, tableName: ADXTableName)
            {
                ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                ReportMethod = IngestionReportMethod.QueueAndTable,
                FlushImmediately = IngestionFlushImmediately,
                Format = DataSourceFormat.json,
                IngestionMapping = new IngestionMapping { IngestionMappingKind = Kusto.Data.Ingestion.IngestionMappingKind.Json, IngestionMappingReference = ADXTableMappingName }

            };
            var sourceOptions = new StorageSourceOptions() { DeleteSourceOnSuccess = DeleteSourceOnSuccessIngestion };
            IRetryPolicy retryPolicy = new NoRetry();
            ((IKustoQueuedIngestClient)Kustoclient).QueuePostRequestOptions.RetryPolicy = retryPolicy;

            //Local Blob creation
            String containerName = BlobContainerName;
            String FilePath = LocalFilePath + containerName + @"\";
            if (!System.IO.Directory.Exists(FilePath))
            {
                System.IO.Directory.CreateDirectory(FilePath);
            }

            //Create Azure Blob Storage connection client
            BlobContainerClient blobContainerClient = new BlobContainerClient(storageConnectionString, containerName);
            await blobContainerClient.CreateIfNotExistsAsync();

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Restart();

            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, queue ingestion demo start");
            String BatchTimestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            String BatchId = Guid.NewGuid().ToString();
            int i = 0;
            while (AccumulatedItems < TotalItems)
            {
                Console.WriteLine();
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Ingestion round {++i}, start");
                int miniItemsPerBatch = ((AccumulatedItems + ItemsPerBatch) > TotalItems ? TotalItems - AccumulatedItems : ItemsPerBatch);
                String miniBatchId = i.ToString().PadLeft(10, '0');
                miniBatchId = miniBatchId.Substring(miniBatchId.Length - 10, 10);
                String FileName = String.Format($"{BatchTimestamp}-{BatchId}-{miniBatchId}.json");
             
                try
                {
                    List<Item> itemslist = GetItemsToList(miniItemsPerBatch, BatchId);
                    await SaveItemsToLocalFilePath(itemslist, FilePath, FileName);
                    await SaveItemsToBlobContainer(FilePath, FileName, blobContainerClient);
                    BlobClient blob = blobContainerClient.GetBlobClient(FileName);
                    Uri blobSASUri;
                    if (DeleteSourceOnSuccessIngestion)
                        blobSASUri = blob.GenerateSasUri(Azure.Storage.Sas.BlobSasPermissions.Read | Azure.Storage.Sas.BlobSasPermissions.Delete, DateTime.UtcNow.AddHours(4));
                    else
                        blobSASUri = blob.GenerateSasUri(Azure.Storage.Sas.BlobSasPermissions.Read, DateTime.UtcNow.AddHours(4));

                    await ExecuteQueueIngestion(blobSASUri, Kustoclient, kustoIngestionProperties, sourceOptions);
                }
                catch (Exception ce)
                {
                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Ingestion round {i}, failed: {ce.Message}");
                }
                finally
                {
                    AccumulatedItems += miniItemsPerBatch;

                    await CleanUpLocalFilePath(FilePath, FileName);
                    if (!DeleteSourceOnSuccessIngestion)
                    {
                        //await CleanUpBlobContainer(blobContainerClient);
                    }

                    Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Ingestion round {i}, finished (AccumulatedItems={AccumulatedItems}).");
                }                
                Thread.Sleep(IngestionDelayMS);
            }

            stopwatch.Stop();
            Console.WriteLine();
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, queue ingestion demo ({TotalItems} items; {i} batches) finished in {stopwatch.Elapsed}.");
        }

        private static async Task ExecuteQueueIngestion(Uri blobSASUri, IKustoQueuedIngestClient Kustoclient, KustoQueuedIngestionProperties kustoIngestionProperties, StorageSourceOptions sourceOptions)
        {
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteQueueIngestion, job start");

            try
            {
                IKustoIngestionResult KustoIngestionresult;
                KustoIngestionresult = Kustoclient.IngestFromStorageAsync(uri: blobSASUri.AbsoluteUri, ingestionProperties: kustoIngestionProperties, sourceOptions).Result;

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
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteQueueIngestion, job submitted: {IngestResults.Count()} ( {Pending} Pending; {Failed} Failed; {Succeeded} Succeeded) / IngestionSourceId:{sourceOptions.SourceId}");
            }
            catch (Exception ce)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteQueueIngestion, job failed: {ce.Message}");
            }
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, ExecuteQueueIngestion, job finished");
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
            catch (Exception ce)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpLocalFilePath, failed: {ce.Message}");
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
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, SaveItemsToBlobContainer, uploadin local file to blob container: {blobContainerClient.Uri}");
            
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
                Kusto Ingestion (queue) is an aync process, we should manually delete blobs after comfirming ingestion completed.
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
            catch (Azure.RequestFailedException ce)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpBlobContainer, failed: {ce.Message}");
            }
            finally
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, CleanUpBlobContainer, finished");
            }
        }

        private static List<Item> GetItemsToList(int ItemCount, String BatchId)
        {
            Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, GetItemsToList, generating {ItemCount} items (BatchId = {BatchId}).");
            
            var SampleTags = new Bogus.Faker<tags>()
                .StrictMode(true)
                .RuleFor(o => o.colors, f => f.Commerce.Color())
                .RuleFor(o => o.material, f => f.Commerce.ProductMaterial());

            int Serial = 1;
            var SampleItems = new Bogus.Faker<Item>()
                .StrictMode(false)
                .RuleFor(o => o.ItemBatch, f => BatchId)
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
