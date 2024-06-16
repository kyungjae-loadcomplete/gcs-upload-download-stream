using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Http;
using Google.Apis.Storage.v1;
using Google.Cloud.Storage.V1;

namespace Utilities.GCP
{
    public class GcsDownloadStream : Stream
    {
        private const int PREFERRED_BUFFER_SIZE = 10 * 1024 * 1024;
        private byte[] _gcsBuffer;
        private MemoryStream _gcsBufferStream = null;
        private readonly StorageClient _client;
        private readonly string _bucketName;
        private readonly string _objectName;
        private long _downloadStreamPosition;
        private long _objectSize;


        public GcsDownloadStream(StorageClient client, string bucketName, string objectName, int bufferSize = PREFERRED_BUFFER_SIZE)
        {
            _client = client;
            _bucketName = bucketName;
            _objectName = objectName;
            _downloadStreamPosition = 0;
            _gcsBuffer = new byte[bufferSize];

            var storageObject = _client.GetObject(bucketName, objectName);
            _objectSize = 0;
            if (storageObject.Size.HasValue)
                _objectSize = (long)(storageObject.Size.Value);
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _objectSize;

        public override long Position
        {
            get => _downloadStreamPosition;
            set => throw new NotSupportedException();
        }

        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_downloadStreamPosition >= _objectSize)
                return 0;

            if (_gcsBufferStream == null || _gcsBufferStream.Position == _gcsBufferStream.Length)
            {
                if (_gcsBufferStream == null)
                    _gcsBufferStream = new MemoryStream(_gcsBuffer);


                // https://cloud.google.com/storage/docs/samples/storage-download-byte-range#storage_download_byte_range-csharp
                StorageService storage = _client.Service;
                var uri = new Uri($"{storage.BaseUri}b/{_bucketName}/o/{Uri.EscapeDataString(_objectName)}?alt=media");
                using var request = new HttpRequestMessage { RequestUri = uri };

                // range 0-99 = 100 bytes
                var rangeBefore = _downloadStreamPosition + _gcsBuffer.Length;
                if (rangeBefore > _objectSize)
                    rangeBefore = _objectSize;
                request.Headers.Range = new RangeHeaderValue(_downloadStreamPosition, rangeBefore - 1);

                var token = GetToken(_client).GetAwaiter().GetResult();
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

                var response = storage.HttpClient.Send(request);
                if (response.StatusCode == HttpStatusCode.NotFound)
                    throw new FileNotFoundException();

                _gcsBufferStream.Position = 0;
                response.Content.CopyToAsync(_gcsBufferStream, null).GetAwaiter().GetResult();
                _gcsBufferStream.Position = 0;
            }

            if (_downloadStreamPosition + count > _objectSize)
                count = (int)(_objectSize - _downloadStreamPosition);
            int bytesRead = _gcsBufferStream.Read(buffer, offset, count);
            _downloadStreamPosition += bytesRead;

            return bytesRead;
        }

        private static async Task<String> GetToken(StorageClient client)
        {
            var credential = client.Service.HttpClientInitializer as GoogleCredential;
            if (credential == null && client.Service.HttpClientInitializer is IConfigurableHttpClientInitializer initializer)
                credential = GoogleCredential.FromAccessToken(await ((ServiceAccountCredential)initializer).GetAccessTokenForRequestAsync());

            if (credential.IsCreateScopedRequired)
                credential = credential.CreateScoped(StorageService.Scope.DevstorageReadOnly);

            var token = await credential.UnderlyingCredential.GetAccessTokenForRequestAsync();
            return token;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _gcsBufferStream?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
