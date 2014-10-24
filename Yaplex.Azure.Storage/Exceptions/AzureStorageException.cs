using System;

namespace Yaplex.Azure.Storage.Exceptions
{
    public class AzureStorageException : ApplicationException
    {
        public AzureStorageException(string message):base(message)
        {
        }
    }
}