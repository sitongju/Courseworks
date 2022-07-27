/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{//这里继承了mapper类
    
    private final static IntWritable one = new IntWritable(1);//这个声明的one就是<key, value>的value, 就是1
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	//if (key.toString().equals("0")) {
    	//map的输入key为当前行在文件内的位置偏移量，所以首行的偏移量肯定是0，所以可以进行如下判断来跳过第一行的处理
		//	return;
		//} else {
			word.set(value.toString().split(",")[2]);
	    	context.write(word, one);// 这个one就是上面声明的，就是1
		//}
       
    }
  }
  
  /**
   * 
   * reducer将中间输出键值对中那些键相同的合并，值为集合。
   * reduce主要有三个阶段：
   *	1.shuffle洗牌:reducer将排序好的输出从每个mapper里面复制出来，整个过程用HTTP来通信。
   *	2.sort排序:框架将reducer的具有相同键的输入合并排序，因为不同的 mapper 可能有相同的键,shuffle过程和sort过程是同时进行的。
   *	3.Reduce归约：在reduce阶段，每个key传进来，reduce方法都被调用一次，进行归约。
   */
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    //此时传进来的value是一个值的集合 <hello,[1,1,1]>
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();//用for循环将value值累加
      }
      result.set(sum);//把sum值赋给result
      context.write(key, result);//输出<hello,3>
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();//配置对象conf,可以配置mapreduce的参数，不过这里没有配置别的
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count"); //新建一个job 用于控制工作流程
    job.setJarByClass(Part.class); //设置工作类名 Part
    job.setMapperClass(TokenizerMapper.class);//设置mapper的类 TokenizerMapper.class
    job.setCombinerClass(IntSumReducer.class);//设置combiner的类 IntSumReducer.class
    job.setReducerClass(IntSumReducer.class);//设置reducer的类 IntSumReducer.class
    job.setOutputKeyClass(Text.class);//设置输出key的类型 text 
    job.setOutputValueClass(IntWritable.class);//设置输出value的格式 int类型  输出整体是这样的： hello 2
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i])); //设置输入路径
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));//设置输出路径
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

