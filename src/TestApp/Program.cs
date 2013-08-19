using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.WindowsAzure;
using System.Diagnostics;
using System.ComponentModel;
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
        public static bool fExit = false;

        static void Main(string[] args)
        {

            // default AzureDirectory stores cache in local temp folder
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            CloudStorageAccount.TryParse(CloudConfigurationManager.GetSetting("blobStorage"), out cloudStorageAccount);
            //AzureDirectory azureDirectory = new AzureDirectory(cloudStorageAccount, "TestTest", new RAMDirectory());
            //AzureDirectory azureDirectory = new AzureDirectory(cloudStorageAccount, "TestTest", FSDirectory.Open(@"c:\test"));
            AzureDirectory azureDirectory = new AzureDirectory(cloudStorageAccount, "TestTest" /* default is FSDirectory.Open(@"%temp%/AzureDirectory/TestTest"); */ );

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
            };
            Console.WriteLine("IndexWriter lock obtained, this process has exclusive write access to index");
            //indexWriter.setRAMBufferSizeMB(10.0);
            //indexWriter.SetUseCompoundFile(false);
            //indexWriter.SetMaxMergeDocs(10000);
            //indexWriter.SetMergeFactor(100);

            for (int iDoc = 0; iDoc < 10000; iDoc++)
            {
                if (iDoc % 10 == 0)
                    Console.WriteLine(iDoc);
                Document doc = new Document();
                doc.add(new Field("id", DateTime.Now.ToFileTimeUtc().ToString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
                doc.add(new Field("Title", GeneratePhrase(10), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
                doc.add(new Field("Body", GeneratePhrase(40), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
                indexWriter.addDocument(doc);
            }
            Console.WriteLine("Total docs is {0}", indexWriter.numDocs());
            
            Console.WriteLine("done");
            Console.WriteLine("Hit Key to search again");
            Console.ReadKey();

            IndexSearcher searcher;
            using (new AutoStopWatch("Creating searcher"))
            {
                searcher = new IndexSearcher(DirectoryReader.open(azureDirectory));
            }
            SearchForPhrase(searcher, "dog");
            SearchForPhrase(searcher, _random.Next(32768).ToString());
            SearchForPhrase(searcher, _random.Next(32768).ToString());
            Console.WriteLine("Hit a key to dispose and exit");
            Console.ReadKey();

            Console.Write("Flushing and disposing writer...");
            // Potentially Expensive: this ensures that all writes are commited to blob storage
            indexWriter.commit();
            indexWriter.close();
        }


        static void SearchForPhrase(IndexSearcher searcher, string phrase)
        {
            using (new AutoStopWatch(string.Format("Search for {0}", phrase)))
            {
                QueryParser parser = new QueryParser(org.apache.lucene.util.Version.LUCENE_CURRENT, "Body", new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_CURRENT));
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

        static Random _random = new Random((int)DateTime.Now.Ticks);
        static string[] sampleTerms =
            { 
                "dog","cat","car","horse","door","tree","chair","microsoft","apple","adobe","google","golf","linux","windows","firefox","mouse","hornet","monkey","giraffe","computer","monitor",
                "steve","fred","lili","albert","tom","shane","gerald","chris",
                "love","hate","scared","fast","slow","new","old"
            };

        private static string GeneratePhrase(int MaxTerms)
        {
            StringBuilder phrase = new StringBuilder();
            int nWords = 2 + _random.Next(MaxTerms);
            for (int i = 0; i < nWords; i++)
            {
                phrase.AppendFormat(" {0} {1}", sampleTerms[_random.Next(sampleTerms.Length)], _random.Next(32768).ToString());
            }
            return phrase.ToString();
        }

    }
    public class AutoStopWatch : IDisposable
    {
        private Stopwatch _stopwatch;
        private string _message;
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

            Debug.WriteLine(String.Format("{0} Finished {1} ms", _message, ms));
        }
        #endregion
    }


}

