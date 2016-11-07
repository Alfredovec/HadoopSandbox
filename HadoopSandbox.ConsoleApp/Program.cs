using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using Microsoft.Hadoop.MapReduce;

namespace HadoopSandbox.ConsoleApp
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Environment.SetEnvironmentVariable("HADOOP_HOME", "abc");
            Environment.SetEnvironmentVariable("JAVA_HOME", "abc");

            //Environment.SetEnvironmentVariable("HADOOP_HOME", @"C:\apps1\dist\hadoop-2.7.1.2.3.3.1-25");
            //Environment.SetEnvironmentVariable("JAVA_HOME", @"C:\apps1\dist\java");

            var hadoop = Hadoop.MakeAzure(
                new Uri("https://sergeyrud.azurehdinsight.net"),
                "sergeyrud", "sergeyrud", "Password1!", "sergeyrud.blob.core.windows.net",
                "D8P7p1KoEvb/aMtfNmYvVwZ2h3p2JW9GMUao2G7y/hKILCAziAWEQ3uw28kISa5TEs/cO+fI9iE6a3AnP6Hkbw==",
                "sergeyrud", false);
            try
            {
                hadoop.MapReduceJob.ExecuteJob<ProcessingJob>();
            }
            catch (AggregateException e) when (e.InnerExceptions.SingleOrDefault() is HttpRequestException)
            {
                Console.WriteLine("Master, it happened. Again.");
            }
        }

        public class Mapper : MapperBase
        {
            public override void Map(string inputLine, MapperContext context)
            {
                context.EmitKeyValue(inputLine, "1");
            }
        }

        public class Reducer : ReducerCombinerBase
        {
            public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
            {
                var totalCount = 0;

                values.ToList().ForEach(v => totalCount += int.Parse(v));

                context.EmitKeyValue(key, "- Total count: " + totalCount);
            }
        }

        public class ProcessingJob : HadoopJob<Mapper, Reducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                var config = new HadoopJobConfiguration
                {
                    InputPath = "wasb://sergeyrud@sergeyrud.blob.core.windows.net/input",
                    OutputFolder = "wasb://sergeyrud@sergeyrud.blob.core.windows.net/output",
                    DeleteOutputFolder = false
                };

                return config;
            }
        }
    }
}
