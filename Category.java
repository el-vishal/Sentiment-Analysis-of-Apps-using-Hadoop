/* FINAL WORKING!

  Vishal Sharma
  Internet & Cloud Computing Assignment 3, Task 1

  The mapper extracts only the relevant information for output from the input file and map them. 
  Mapper sends App_category as key and (Type, Price, count(one)) as values.
  
  The reducer then counts explicitly for Free and Paid apps and calculates the Paid apps average after that.
  After which outputs Category as key and Count(Free), Count(Paid), Avg(Paid) apps as value.
  
*/

package org.leicester;

//Java library files
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.ArrayList;
import java.text.DecimalFormat;

//hadoop library files
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Category {
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
  	 public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
		 private Text category_key = new Text();
		 private Text category_value = new Text();
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 // split a line from a csv file into fields, returned as String-array
			 String S = value.toString();
			 String[] A = splitbycomma(value.toString());

			 // skip empty lines and header lines
			 if (A.length==0 || A[0].equals("App")) return;

			 //assigning tuples to variables in each iteration
			 String categoryStr = A[1];
			 String typeStr = A[6];
			 String priceStr = A[7];
			 int count_value = 1;
			 
			 //Assigning key and values
			 String comb_values = String.valueOf(typeStr+","+count_value+","+priceStr); //Concatenate multiple values as single string, separated by comma.
			 category_key.set(categoryStr);
			 category_value.set(comb_values);
			 context.write(category_key,category_value);

      		}


    	}
	 //Reducer gets key,values from Mapper in the form of Text. It also outputs in Text,Text format.
	 public static class DateReducer extends Reducer<Text,Text,Text,Text> {
      	 private Text cat_name = new Text();
		 private Text result = new Text();

		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 
			 //initializing variables
			 int count_free = 0;
			 int count_paid = 0;
			 float price_sum = 0;
			 float paid_avg = 0;

			 for (Text val : values) {
				 //Seperate the values by splitting it with symbol comma.
				 String line = val.toString();
				 String[] field = line.split(",");

				 //assigning tuples to variables in each iteration
				 String cat_type = field[0];
				 String int_amount = field[2].replaceAll("[$]", ""); //Removing $ symbol from price to perform arithmetic.
				 
				 //If app is paid, increase paid count and add up sum based on the key.
				 if(cat_type.equals("Paid")){
					 count_paid += Integer.parseInt(field[1]);
					 price_sum += Float.parseFloat(int_amount);
					}
				 //Else increase count of free.
				 else{
					 count_free += Integer.parseInt(field[1]);
					}


				}
 	      	 if(count_paid == 0){
				paid_avg = 0; //To avoid divide by 0 error.
			 }
			 else{
				 paid_avg = price_sum/count_paid; //Calculate average price.
				}
			 //Assigning key and values
			 String S = key.toString();
		     cat_name.set(S);
		     DecimalFormat df = new DecimalFormat("###.##"); //Rounding decimal to 2 digit
			 String paid_avg_s = df.format(paid_avg);
			 String v = String.valueOf(count_free)+", \t"+String.valueOf(count_paid)+", \t $"+String.valueOf(paid_avg_s); //Multiple values concatenation in to one string.
			 result.set(v);
			 context.write(cat_name,result); //Output


			}
		}

	 public static void main(String[] args) throws Exception {
   		 Configuration conf = new Configuration();
  		 Job job = Job.getInstance(conf, "Category");
		 job.setJarByClass(Category.class);
		 job.setMapperClass(TokenizerMapper.class); //Mapper class
		 job.setReducerClass(DateReducer.class); //Reducer class
		 job.setOutputKeyClass(Text.class); // output key type for mapper
		 job.setOutputValueClass(Text.class); // output value type for mapper
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
