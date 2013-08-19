using System;
using System.Globalization;
using System.Text;
using System.Threading;
using Microsoft.WindowsAzure;
using System.Diagnostics;
using Lucene.Net.Store.Azure;
using Microsoft.WindowsAzure.Storage;
using org.apache.lucene.analysis.standard;
using org.apache.lucene.document;
using org.apache.lucene.index;
using org.apache.lucene.queryparser.classic;
using org.apache.lucene.store;
using org.apache.lucene.search;

namespace TestApp
{
    class Program
    {
        public static bool FExit = false;

        static void Main()
        {

            // default AzureDirectory stores cache in local temp folder
            CloudStorageAccount cloudStorageAccount;
            CloudStorageAccount.TryParse(CloudConfigurationManager.GetSetting("blobStorage"), out cloudStorageAccount);
            //AzureDirectory azureDirectory = new AzureDirectory(cloudStorageAccount, "TestTest", new RAMDirectory());
            //AzureDirectory azureDirectory = new AzureDirectory(cloudStorageAccount, "TestTest", FSDirectory.Open(@"c:\test"));
            var azureDirectory = new AzureDirectory(cloudStorageAccount, "TestTest" /* default is FSDirectory.Open(@"%temp%/AzureDirectory/TestTest"); */ );

            IndexWriter indexWriter = null;
            while (indexWriter == null)
            {
                try
                {
                    var config = new IndexWriterConfig(org.apache.lucene.util.Version.LUCENE_CURRENT, new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_CURRENT));
                    indexWriter = new IndexWriter(azureDirectory, config);
                }
                catch (LockObtainFailedException)
                {
                    Console.WriteLine("Lock is taken, waiting for timeout...");
                    Thread.Sleep(1000);
                }
            }
            Console.WriteLine("IndexWriter lock obtained, this process has exclusive write access to index");
            //indexWriter.setRAMBufferSizeMB(10.0);
            //indexWriter.SetUseCompoundFile(false);
            //indexWriter.SetMaxMergeDocs(10000);
            //indexWriter.SetMergeFactor(100);

            for (int iDoc = 0; iDoc < 10000; iDoc++)
            {
                if (iDoc % 10 == 0)
                    Console.WriteLine(iDoc);
                var doc = new Document();
                doc.add(new TextField("id", DateTime.Now.ToFileTimeUtc().ToString(CultureInfo.InvariantCulture), Field.Store.YES));
                doc.add(new TextField("Title", GeneratePhrase(10), Field.Store.YES));
                doc.add(new TextField("Body", GeneratePhrase(40), Field.Store.YES));
                indexWriter.addDocument(doc);
            }
            Console.WriteLine("Total docs is {0}", indexWriter.numDocs());

            Console.Write("Flushing and disposing writer...");
            // Potentially Expensive: this ensures that all writes are commited to blob storage
            indexWriter.commit();
            indexWriter.close();

            Console.WriteLine("done");
            Console.WriteLine("Hit Key to search again");
            Console.ReadKey();

            IndexSearcher searcher;
            using (new AutoStopWatch("Creating searcher"))
            {
                searcher = new IndexSearcher(DirectoryReader.open(azureDirectory));
            }
            SearchForPhrase(searcher, "dog");
            SearchForPhrase(searcher, Random.Next(32768).ToString(CultureInfo.InvariantCulture));
            SearchForPhrase(searcher, Random.Next(32768).ToString(CultureInfo.InvariantCulture));
            Console.WriteLine("Hit a key to dispose and exit");
            Console.ReadKey();
        }

        static void SearchForPhrase(IndexSearcher searcher, string phrase)
        {
            using (new AutoStopWatch(string.Format("Search for {0}", phrase)))
            {
                var parser = new QueryParser(org.apache.lucene.util.Version.LUCENE_CURRENT, "Body", new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_CURRENT));
                Query query = parser.parse(phrase);

                var hits = searcher.search(query, 100);
                Console.WriteLine("Found {0} results for {1}", hits.totalHits, phrase);
                int max = hits.totalHits;
                if (max > 100)
                    max = 100;
                for (int i = 0; i < max; i++)
                {
                    Console.WriteLine(hits.scoreDocs[i].doc);
                }
            }
        }

        static readonly Random Random = new Random((int)DateTime.Now.Ticks);
        static readonly string[] SampleTerms =
            { 
                "dog","cat","car","horse","door","tree","chair","microsoft","apple","adobe","google","golf","linux","windows","firefox","mouse","hornet","monkey","giraffe","computer","monitor",
                "steve","fred","lili","albert","tom","shane","gerald","chris",
                "love","hate","scared","fast","slow","new","old"
            };

        private static string GeneratePhrase(int maxTerms)
        {
            var phrase = new StringBuilder();
            int nWords = 2 + Random.Next(maxTerms);
            for (int i = 0; i < nWords; i++)
            {
                phrase.AppendFormat(" {0} {1}", SampleTerms[Random.Next(SampleTerms.Length)], Random.Next(32768));
            }
            return phrase.ToString();
        }
    }

    public class AutoStopWatch : IDisposable
    {
        private readonly Stopwatch _stopwatch;
        private readonly string _message;
        public AutoStopWatch(string message)
        {
            _message = message;
            Debug.WriteLine(String.Format("{0} starting ", message));
            _stopwatch = Stopwatch.StartNew();
        }

        #region IDisposable Members
        public void Dispose()
        {

            _stopwatch.Stop();
            long ms = _stopwatch.ElapsedMilliseconds;

            Debug.WriteLine("{0} Finished {1} ms", _message, ms);
        }
        #endregion
    }
}