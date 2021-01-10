using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace AzureAPIs
{
    public static class AzureBlobStorage
    {
        public static bool UploadFileByXml(
            string accountName,
            string accountKey,
            string containerName,
            string fileXml,
            string category,
            string uploadedBy,
            DateTime date,
            string userRole,
            Guid? serviceImplId = null,
            Guid? organizationUnitId = null,
            Guid? indicatorCode = null)
        {
            try
            {
                // parse fileXml
                /*    <file>
                        <name>File_name</name>
                        <content>BASE64encodedContent</content>
                      </file>
                */

                XDocument doc = XDocument.Parse(fileXml);

                XElement nameElement = doc.Root.Element("name");
                string fileName = nameElement.Value;

                XElement contentElement = doc.Root.Element("content");
                string base64Content = contentElement.Value;

                string connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net";
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

                CloudBlobContainer container = blobClient.GetContainerReference(containerName);

                if (!container.Exists())
                {
                    container.CreateIfNotExists();
                }

                CloudBlockBlob blob = container.GetBlockBlobReference(fileName);

                blob.Metadata.Add(Constants.CategoryLabel, category);
                blob.Metadata.Add(Constants.UploadedByLabel, uploadedBy);
                blob.Metadata.Add(Constants.DateLabel, date.ToString("MM/dd/yyyy HH:mm:ss tt"));
                blob.Metadata.Add(Constants.UserRoleLabel, userRole);

                if (serviceImplId.HasValue && serviceImplId != Guid.Empty)
                {
                    blob.Metadata.Add(Constants.ServiceImplIdLabel, serviceImplId.Value.ToString());
                }

                if (organizationUnitId.HasValue && organizationUnitId != Guid.Empty)
                {
                    blob.Metadata.Add(Constants.OrganizationUnitIdLabel, organizationUnitId.Value.ToString());
                }

                if (indicatorCode.HasValue && indicatorCode != Guid.Empty)
                {
                    blob.Metadata.Add(Constants.IndicatorCodeLabel, indicatorCode.Value.ToString());
                }

                byte[] fileBytes = Convert.FromBase64String(base64Content);

                blob.UploadFromByteArray(fileBytes, 0, fileBytes.Length);
            }
            catch
            {
                return false;
            }

            return true;
        }

         public static bool UploadFileByFullPath(
            string accountName,
            string accountKey,
            string containerName,
            string fullPath,
            string fileName,
            string category,
            string uploadedBy,
            DateTime date,
            string userRole,
            Guid? serviceImplId = null,
            Guid? organizationUnitId = null,
            Guid? indicatorCode = null)
        {
            try
            {
                string connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net";
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

                CloudBlobContainer container = blobClient.GetContainerReference(containerName);

                if (!container.Exists())
                {
                    container.CreateIfNotExists();
                }

                CloudBlockBlob blob = container.GetBlockBlobReference(fileName);

                blob.Metadata.Add(Constants.CategoryLabel, category);
                blob.Metadata.Add(Constants.UploadedByLabel, uploadedBy);
                blob.Metadata.Add(Constants.DateLabel, date.ToString("MM/dd/yyyy HH:mm:ss tt"));
                blob.Metadata.Add(Constants.UserRoleLabel, userRole);

                if (serviceImplId.HasValue && serviceImplId != Guid.Empty)
                {
                    blob.Metadata.Add(Constants.ServiceImplIdLabel, serviceImplId.Value.ToString());
                }

                if (organizationUnitId.HasValue && organizationUnitId != Guid.Empty)
                {
                    blob.Metadata.Add(Constants.OrganizationUnitIdLabel, organizationUnitId.Value.ToString());
                }

                if (indicatorCode.HasValue && indicatorCode != Guid.Empty)
                {
                    blob.Metadata.Add(Constants.IndicatorCodeLabel, indicatorCode.Value.ToString());
                }

                blob.UploadFromFile(fullPath);
            }
            catch
            {
                return false;
            }

            return true;
        }

        public static bool UploadFileByFullPathUsingToken(
            string accountName,
            string accountToken,
            string containerName,
            string fullPath,
            string fileName,
            string category = null,
            string uploadedBy = null,
            DateTime? date = null,
            string userRole = null,
            string serviceImplId = null,
            string organizationUnitId = null,
            string indicatorCode = null,
            string fileDescription = null)
        {
            try
            {
                var uri = new Uri($"https://{accountName}.blob.core.windows.net/{containerName}?{accountToken}");
                CloudBlobContainer container = new CloudBlobContainer(uri);

                if (!container.Exists())
                {
                    container.CreateIfNotExists();
                }

                CloudBlockBlob blob = container.GetBlockBlobReference(fileName);

                if (!string.IsNullOrEmpty(category))
                {
                    blob.Metadata.Add(Constants.CategoryLabel, category);
                }
                
                if (!string.IsNullOrEmpty(uploadedBy))
                {
                    blob.Metadata.Add(Constants.UploadedByLabel, uploadedBy);
                }
                
                if (date.HasValue)
                {
                    blob.Metadata.Add(Constants.DateLabel, date.Value.ToString("MM/dd/yyyy HH:mm:ss tt"));
                }

                if (!string.IsNullOrEmpty(userRole))
                {
                    blob.Metadata.Add(Constants.UserRoleLabel, userRole);
                }

                if (!string.IsNullOrEmpty(serviceImplId))
                {
                    blob.Metadata.Add(Constants.ServiceImplIdLabel, serviceImplId);
                }

                if (!string.IsNullOrEmpty(organizationUnitId))
                {
                    blob.Metadata.Add(Constants.OrganizationUnitIdLabel, organizationUnitId);
                }

                if (!string.IsNullOrEmpty(indicatorCode))
                {
                    blob.Metadata.Add(Constants.IndicatorCodeLabel, indicatorCode);
                }

                if (!string.IsNullOrEmpty(fileDescription))
                {
                    blob.Metadata.Add(Constants.FileDescriptionLabel, fileDescription);
                }

                blob.UploadFromFile(fullPath);
            }
            catch (Exception ex)
            {
                return false;
            }

            return true;
        }

        public static bool UploadFile(
            string accountName,
            string accountKey,
            string containerName,
            Stream stream,
            string fileName,
            string category,
            string uploadedBy,
            DateTime date,
            string userRole,
            string serviceImplId,
            string organizationUnitId,
            string indicatorCode)
        {
            try
            {
                string connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net";
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

                CloudBlobContainer container = blobClient.GetContainerReference(containerName);

                if (!container.Exists())
                {
                    container.CreateIfNotExists();
                }

                CloudBlockBlob blob = container.GetBlockBlobReference(fileName);

                blob.Metadata.Add(Constants.CategoryLabel, category);
                blob.Metadata.Add(Constants.UploadedByLabel, uploadedBy);
                blob.Metadata.Add(Constants.DateLabel, date.ToString("MM/dd/yyyy HH:mm:ss tt"));
                blob.Metadata.Add(Constants.UserRoleLabel, userRole);
               
                if (!string.IsNullOrEmpty(serviceImplId))
                {
                    blob.Metadata.Add(Constants.ServiceImplIdLabel, serviceImplId);
                }

                if (!string.IsNullOrEmpty(organizationUnitId))
                {
                    blob.Metadata.Add(Constants.OrganizationUnitIdLabel, organizationUnitId);
                }
               
                if (!string.IsNullOrEmpty(indicatorCode))
                {
                    blob.Metadata.Add(Constants.IndicatorCodeLabel, indicatorCode);
                }
               
                blob.UploadFromStream(stream);
            }
            catch (Exception ex)
            {
                return false;
            }

            return true;
        }

        public static List<BlobFileInfo> GetFilesFromContainer(
            string accountName, 
            string accountKey, 
            string containerName, 
            string userRole,
            Guid? serviceImplId,
            Guid? organizationUnitId,
            Guid? indicatorCode)
        {
            string connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            var blobs = container.ListBlobs().OfType<CloudBlockBlob>().ToList();

            container.FetchAttributes();

            var myFiles = new List<BlobFileInfo>();

            foreach (var blob in blobs)
            {
                blob.FetchAttributes();

                var fileInfo = new BlobFileInfo
                {
                    FileName = blob.Name,
                };

                if (blob.Metadata != null)
                {
                    foreach (var kvp in blob.Metadata)
                    {
                        switch (kvp.Key)
                        {
                            case Constants.CategoryLabel:
                                fileInfo.Category = kvp.Value;
                                break;

                            case Constants.DateLabel:
                                fileInfo.Date = DateTime.Parse(kvp.Value);
                                break;

                            case Constants.UserRoleLabel:
                                fileInfo.UserRole = kvp.Value;
                                break;

                            case Constants.ServiceImplIdLabel:
                                fileInfo.ServiceImplId = kvp.Value;
                                break;

                            case Constants.OrganizationUnitIdLabel:
                                fileInfo.OrganizationUnitId = kvp.Value;
                                break;

                            case Constants.IndicatorCodeLabel:
                                fileInfo.IndicatorCode = kvp.Value;
                                break;
                        }
                    }
                }

                myFiles.Add(fileInfo);
            }

            string servImpId = serviceImplId.HasValue ? serviceImplId.Value.ToString() : string.Empty;
            string orgId = organizationUnitId.HasValue ? organizationUnitId.Value.ToString() : string.Empty;
            string indCode = indicatorCode.HasValue ? indicatorCode.Value.ToString() : string.Empty;

            return myFiles.Where(
                            x => (String.IsNullOrEmpty(userRole) || String.Compare(userRole, x.UserRole, StringComparison.InvariantCultureIgnoreCase) == 0) &&
                                 (String.IsNullOrEmpty(servImpId) || String.Compare(servImpId, x.ServiceImplId, StringComparison.InvariantCultureIgnoreCase) == 0) &&
                                 (String.IsNullOrEmpty(orgId) || String.Compare(orgId, x.OrganizationUnitId, StringComparison.InvariantCultureIgnoreCase) == 0) &&
                                 (String.IsNullOrEmpty(indCode) || String.Compare(indCode, x.IndicatorCode, StringComparison.InvariantCultureIgnoreCase) == 0)
                            ).ToList() ;
        }

        public static List<BlobFileInfo> GetFilesFromContainerUsingToken(
            string accountName,
            string accountToken,
            string containerName,
            string userName = null,
            string userRole = null,
            string serviceImplId = null,
            string organizationUnitId = null,
            string indicatorCode = null)
        {
            var uri = new Uri($"https://{accountName}.blob.core.windows.net/{containerName}?{accountToken}");
            CloudBlobContainer container = new CloudBlobContainer(uri);

            var blobs = container.ListBlobs().OfType<CloudBlockBlob>().ToList();

            container.FetchAttributes();

            var myFiles = new List<BlobFileInfo>();

            foreach (var blob in blobs) 
            {
                blob.FetchAttributes();

                var fileInfo = new BlobFileInfo
                {
                    FileName = blob.Name,
                };

                if (blob.Metadata != null)
                {
                    foreach (var kvp in blob.Metadata)
                    {
                        switch (kvp.Key)
                        {
                            case Constants.CategoryLabel:
                                fileInfo.Category = kvp.Value;
                                break;

                            case Constants.FileDescriptionLabel:
                                fileInfo.FileDescription = kvp.Value;
                                break;

                            case Constants.DateLabel:
                                fileInfo.Date = DateTime.Parse(kvp.Value);
                                break;

                            case Constants.UploadedByLabel:
                                fileInfo.UserName = kvp.Value;
                                break;

                            case Constants.UserRoleLabel:
                                fileInfo.UserRole = kvp.Value;
                                break;

                            case Constants.ServiceImplIdLabel:
                                fileInfo.ServiceImplId = kvp.Value;
                                break;

                            case Constants.OrganizationUnitIdLabel:
                                fileInfo.OrganizationUnitId = kvp.Value;
                                break;

                            case Constants.IndicatorCodeLabel:
                                fileInfo.IndicatorCode = kvp.Value;
                                break;
                        }
                    }
                }

                myFiles.Add(fileInfo);
            }

            userName = userName ?? string.Empty;
            userRole = userRole ?? string.Empty;
            string servImpId = serviceImplId ?? string.Empty;
            string orgId = organizationUnitId ?? string.Empty;
            string indCode = indicatorCode ?? string.Empty;

            return myFiles.Where(
                           x => (String.IsNullOrEmpty(userName) || String.Compare(userName, x.UserName, StringComparison.InvariantCultureIgnoreCase) == 0) &&
                                (String.IsNullOrEmpty(userRole) || String.Compare(userRole, x.UserRole, StringComparison.InvariantCultureIgnoreCase) == 0) &&
                                (String.IsNullOrEmpty(servImpId) || String.Compare(servImpId, x.ServiceImplId, StringComparison.InvariantCultureIgnoreCase) == 0) &&
                                (String.IsNullOrEmpty(orgId) || String.Compare(orgId, x.OrganizationUnitId, StringComparison.InvariantCultureIgnoreCase) == 0) &&
                                (String.IsNullOrEmpty(indCode) || String.Compare(indCode, x.IndicatorCode, StringComparison.InvariantCultureIgnoreCase) == 0)
                           ).ToList();
        }

        public static List<string> GetFilesFromContainerV2(string accountName, string accountKey, string containerName)
        {
            string connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            var blobs = container.ListBlobs().OfType<CloudBlockBlob>().ToList();

            container.FetchAttributes();

            var myFiles = new List<string>();

            foreach (var blob in blobs)
            {
                blob.FetchAttributes();

                myFiles.Add(blob.Name);
            }

            return myFiles;
        }

        public static MyBlobFile DownloadFileFromContainer(string accountName, string accountKey, string containerName, string fileName)
        {
            MyBlobFile file = null;

            string connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            var blobs = container.ListBlobs().OfType<CloudBlockBlob>().ToList();

            var blob = blobs.FirstOrDefault(x => string.Compare(x.Name, fileName, System.StringComparison.InvariantCultureIgnoreCase) == 0);

            if (blob != null)
            {
                byte[] fileContent = new byte[blob.Properties.Length];

                blob.DownloadToByteArray(fileContent, 0);

                blob.FetchAttributes();

                file = new MyBlobFile
                {
                    FileName = fileName,
                    FileContent = fileContent,
                };

                if (blob.Metadata != null)
                {
                    foreach (var kvp in blob.Metadata)
                    {
                        switch (kvp.Key)
                        {
                            case Constants.CategoryLabel:
                                file.Category = kvp.Value;
                                break;

                            case Constants.UploadedByLabel:
                                file.UploadedBy = kvp.Value;
                                break;

                            case Constants.DateLabel:
                                file.Date = DateTime.Parse(kvp.Value);
                                break;

                            case Constants.UserRoleLabel:
                                file.UserRole = kvp.Value;
                                break;
                        }
                    }
                }
            }

            return file;
        }

        public static MyBlobFile DownloadFileFromContainerUsingToken(string accountName, string accountToken, string containerName, string fileName)
        {
            MyBlobFile file = null;

            var uri = new Uri($"https://{accountName}.blob.core.windows.net/{containerName}?{accountToken}");
            CloudBlobContainer container = new CloudBlobContainer(uri);

            var blobs = container.ListBlobs().OfType<CloudBlockBlob>().ToList();

            var blob = blobs.FirstOrDefault(x => string.Compare(x.Name, fileName, System.StringComparison.InvariantCultureIgnoreCase) == 0);

            if (blob != null)
            {
                byte[] fileContent = new byte[blob.Properties.Length];

                blob.DownloadToByteArray(fileContent, 0);

                blob.FetchAttributes();

                file = new MyBlobFile
                {
                    FileName = fileName,
                    FileContent = fileContent,
                };

                if (blob.Metadata != null)
                {
                    foreach (var kvp in blob.Metadata)
                    {
                        switch (kvp.Key)
                        {
                            case Constants.CategoryLabel:
                                file.Category = kvp.Value;
                                break;

                            case Constants.UploadedByLabel:
                                file.UploadedBy = kvp.Value;
                                break;

                            case Constants.DateLabel:
                                file.Date = DateTime.Parse(kvp.Value);
                                break;

                            case Constants.UserRoleLabel:
                                file.UserRole = kvp.Value;
                                break;
                        }
                    }
                }
            }

            return file;
        }

        public static bool DeleteFile(string accountName, string accountKey, string originalContainer, string containerForDeletes, string fileName)
        {
            string connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference(originalContainer);

            var blobs = container.ListBlobs().OfType<CloudBlockBlob>().ToList();

            var sourceBlob = blobs.FirstOrDefault(x => string.Compare(x.Name, fileName, System.StringComparison.InvariantCultureIgnoreCase) == 0);

            if (sourceBlob != null)
            {
                CloudBlobContainer deleteContainer = blobClient.GetContainerReference(containerForDeletes);

                if (!deleteContainer.Exists())
                {
                    deleteContainer.CreateIfNotExists();
                }

                CloudBlockBlob deleteBlob = deleteContainer.GetBlockBlobReference(fileName);

                deleteBlob.StartCopy(sourceBlob);

                if (deleteBlob.Exists())
                {
                    sourceBlob.Delete();
                }
                else
                {
                    throw new FileNotFoundException();
                }
            }
            else
            {
                throw new FileNotFoundException();
            }

            return true;
        }
        
        public static bool DeleteFileUsingToken(string accountName, string accountToken, string originalContainer, string containerForDeletes, string fileName)
        {            
            CloudBlobContainer sourceContainer = new CloudBlobContainer(new Uri($"https://{accountName}.blob.core.windows.net/{originalContainer}?{accountToken}"));

            var blobs = sourceContainer.ListBlobs().OfType<CloudBlockBlob>().ToList();
            var sourceBlob = blobs.FirstOrDefault(x => string.Compare(x.Name, fileName, System.StringComparison.InvariantCultureIgnoreCase) == 0);

            if (sourceBlob != null)
            {
                CloudBlobContainer deleteContainer = 
                    new CloudBlobContainer(new Uri($"https://{accountName}.blob.core.windows.net/{containerForDeletes}?{accountToken}"));

                if (!deleteContainer.Exists())
                {
                    deleteContainer.CreateIfNotExists();
                }

                CloudBlockBlob deleteBlob = deleteContainer.GetBlockBlobReference(fileName);

                deleteBlob.StartCopy(sourceBlob);

                if (deleteBlob.Exists())
                {
                    sourceBlob.Delete();
                }
                else
                {
                    throw new FileNotFoundException();
                }
            }
            else
            {
                throw new FileNotFoundException();
            }

            return true;
        }
    }
}