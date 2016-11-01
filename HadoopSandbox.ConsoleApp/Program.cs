using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Hadoop.MapReduce;

namespace HadoopSandbox.ConsoleApp
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect(
                new Uri("https://sergiirud.azurehdinsight.net"),
                "admin", "hdp", "admin", "sergiirud.blob.core.windows.net",
                "VnYzB0FhQ6JSwrWF2e/i2xTQ8u5WOkuy2qLopcWgp5ap7V+vfzmoaHBs/HY+tMExWz+qtc9A9IxuhS8RIX4t4w==",
                "sergiirud", false);
            
            hadoop.MapReduceJob.ExecuteJob<ProcessingJob>();
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

                context.EmitKeyValue("key", totalCount.ToString());
            }
        }

        public class ProcessingJob : HadoopJob<Mapper, Reducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                var config = new HadoopJobConfiguration
                {
                    InputPath = "asv://sergiirud@sergiirud.blob.core.windows.net/input",
                    OutputFolder = "asv://sergiirud@sergiirud.blob.core.windows.net/output",
                    DeleteOutputFolder = true
                };

                return config;
            }
        }
    }
}
