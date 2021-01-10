using System;

namespace AzureAPIs
{
    public class MyBlobFile
    {
        public byte[] FileContent { get; set; }

        public string FileName { get; set; }

        public string Category { get; set; }

        public string UploadedBy { get; set; }

        public DateTime Date { get; set; }

        public string UserRole { get; set; }
    }
}
