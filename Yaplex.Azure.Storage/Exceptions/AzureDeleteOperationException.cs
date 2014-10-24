namespace Yaplex.Azure.Storage.Exceptions
{
    public class AzureDeleteOperationException : AzureStorageException
    {
        public AzureDeleteOperationException(string message) : base(message)
        {
        }
    }
}