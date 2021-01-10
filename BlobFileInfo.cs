using System;

namespace AzureAPIs
{
    public class BlobFileInfo
    {
        public string FileName { get; set; }

        public string Category { get; set; }

        public DateTime Date { get; set; }

        public string UserName { get; set; }

        public string UserRole { get; set; }

        public string FileDescription { get; set; }


        public string ServiceImplId { get; set; }

        public string OrganizationUnitId { get; set; }

        public string IndicatorCode { get; set; }
    }
}
