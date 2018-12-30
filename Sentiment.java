/* FINAL WORKING!

  Vishal Sharma
  Internet & Cloud Computing Assignment 3, Task 2

  Since two files were given as input, concept of Reduce-side join has been used.
  
  The mapper extracts only the relevant information for output from the input files and map them.
  Files are identified using the number of columns they have (which are diffferent). 
  Apps not for "Everyone" as well having "nan" in sentiment_polarity have been eliminated in the mapper itself.
  Mapper sends App_category as key and (Type, Price, count(one)) as values.
  
  The reducer then recieves different values for single key. Thus, join can be performed for each value in both the tables.
  Reducer performs the join and proceeds with mapping of key(app) with category and calculation of reviews, sentiment_polarity.
  After which outputs App as key and (Category, sum(reviews), sentiment_polarity) as value.
  
*/

package org.leicester;

import java.io.IOException;
import java.util.ArrayList;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Sentiment {
	 // split a line from a csv file into fields, returned as String-array
	 public static String[] splitbycomma(String S) {
		ArrayList<String> L = new ArrayList<String>();
			String[] a = new String[0];
			StringBuffer B = new StringBuffer();
			int i = 0;
			while (i<S.length()) {
					int start = i;
					int end=-1;
					if (S.charAt(i)=='"') { // parse field enclosed in quotes
							B.setLength(0); // clear the StringBuffer
							B.append('"');
							i++;
							while (i<S.length()) {
								if (S.charAt(i)!='"') { // not a quote, just add to B
										B.append(S.charAt(i));
										i++;
								}
								else { // quote, need to check if next character is also quote
										if (i+1 < S.length() && S.charAt(i+1)=='"') {
											B.append('"');
											i+=2;
										}
										else { // no, so the quote marked the end of the String
											B.append('"');
											L.add(B.toString());
											i+=2;
											break;
										}
								}
							}
					}
					else { // standard field, extends until next comma
							end = S.indexOf(',',i)-1;
							if (end<0) end = S.length()-1;
							L.add(S.substring(start,end+1));
							i = end+2;
					}
			}
			return L.toArray(a);
	}

	 //Mapper class here only considers of taking the input file as value (Text) and outputs keys and value in form of Text to reducer
	 public static class SentimentMapper
		extends Mapper<Object, Text, Text, Text>{
			 private Text word = new Text();
			 private Text value = new Text();

			 // mapper outputs AppName as key and 1 as value for every url visited by username
			 public void map(Object key, Text value, Context context) 
			 throws IOException, InterruptedException {
	 		 // split a line from a csv file into fields, returned as String-array		
			 String allSheet = value.toString();
			 String[] A = splitbycomma(value.toString());
			
			 // skip empty lines and header lines
			 if (A.length==0 || A[0].equals("App")) return; 
			 //File A identification and pass only rows(app) with type "Everyone"
			 if (A.length==13 && !A[8].trim().equals("Everyone")) return;
			 //File B identification and pass only rows(app) with sentiment_polarity not "nan"
			 if (A.length==5 && A[3].trim().equals("nan")) return;
			
			
			 String A_AppName = "";
			 String A_Category = "";
			 String B_AppName = "";
			 String B_SentimentPol = "";

			 // assigning tuples to variables in each iteration	
			 // File A Reading 	
			 if (A.length==13 && A[8].equals("Everyone")){
				 A_AppName = A[0].toString().trim();
				 A_Category = A[1].toString().trim();
				}
			 // File B Reading 
			 else if (A.length==5 && !A[3].equals("nan")) {
				 B_AppName = A[0].toString().trim();
				 B_SentimentPol = A[3].toString().trim();
				}
			 //Clearing any garbage data
			 else {
				 return;
				}
			
			 //Assigning key and values
			 String fileAValues ="";
			 String fileBValues ="";
			
			 fileAValues = "FileA"+"$@#"+A_Category;
			 fileBValues = "FileB"+"$@#"+B_SentimentPol;
			
			 if (A_AppName != ""){
				 word.set(A_AppName);
				 value.set(fileAValues);		
				}
			 else if (B_AppName != ""){
				 word.set(B_AppName);
				 value.set(fileBValues);
				}
			 else {
				 return;
				}
			 context.write(word,value);
			}
	
		} // Mapper closing parenthesis


	 //Reducer gets key,values from Mapper in the form of Text. It also outputs in Text,Text format.
	 public static class SentimentReducer
	 extends Reducer<Text,Text,Text,Text> {
		 private Text final_value = new Text();
		 
		 public void reduce(Text key, Iterable<Text> values, Context context)
		 throws IOException, InterruptedException {
			 
			 //initializing variables.
			 String cat_name = "";
			 double sentiment_sum = 0.0;
			 double sentiment_avg = 0.0;			 
			 int reviews = 0;
			 for (Text val : values) {
				 //Seperate the values by splitting it with symbol comma.
				 String line = val.toString();
				 String[] field = line.split("\\$@#");
				 
				 //assigning tuples to variables in each iteration.
				 //when file A, just take category.
				 if (field[0].equals("FileA")) {
					 
					 cat_name = field[1];
					 //total += Float.parseFloat(parts[1]);
					} 
				 //when file B, increment count of reviews and sum up sentiment_polarity.
				 else if (field[0].equals("FileB")) {
					 reviews = reviews + 1;
					 sentiment_sum = sentiment_sum + Float.parseFloat(field[1]);
					}
				}
			
			 //Assigning key and values
			 if(reviews >= 50 ){ //checking for output condition - reviews >= 50.
				 sentiment_avg = sentiment_sum / reviews;
				 if(sentiment_avg >= 0.3){ //checking for output condition - avg(sentiment_polarity) >= 0.3.
					 DecimalFormat df = new DecimalFormat("###.##"); //rounding off to two decimal digit.
					 String s_avg = df.format(sentiment_avg);
					 if(cat_name.equals("")){ //if no category defined.
						cat_name = "NOT_DEFINED";
					 }
					 String v = String.valueOf(cat_name+","+reviews+","+s_avg); //multiple values into one string for value.
					 //String str = String.format("%d\t%f", count, total);
					 final_value.set(v);
					 context.write(key, final_value);
					}
				}
			}
		} // Reducer Closing Parenthesis 
  
	
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "Sentiment");
		 job.setJarByClass(Sentiment.class);
		 job.setMapperClass(SentimentMapper.class);
		 //job.setCombinerClass(SentimentCombiner.class);
		 job.setReducerClass(SentimentReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
