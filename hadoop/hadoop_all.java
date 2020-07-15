

/* Exercise 1 - Number of occurrences of each word in the input file 
(ONE job, MANY reducers) */

protected void map(LongWritable key, Text value, Context context) {

	String[] words = value.toString().split("\\s+");
	
	for(String word : words) {
		String cleanedWord = word.toLowerCase();
		
		// emit the pair (word, 1)
		context.write(new Text(cleanedWord), new IntWritable(1));
	}
}

protected void reduce(Text key, Iterable<IntWritable> values, Context context){

	int occurrences = 0;

	// Iterate over the set of values and sum them 
	for (IntWritable value : values) {
		occurrences = occurrences + value.get();
	}
	
	context.write(key, new IntWritable(occurrences));
}

/* Exercise 3 - couples (sensorId, number of days) with a PM10 value 
above a specific theshold (50) (ONE job, MANY reducers) */

private static Double PM10Threshold = new Double(50);
	
protected void map(LongWritable key, Text value, Context context)  {

		// Extract sensor and date from the key
		String[] fields = key.toString().split(",");
		
		String sensor_id=fields[0];
		Double PM10Level=new Double(value.toString());
		
		// Compare the value of PM10 with the threshold value
		if (PM10Level>PM10Threshold){
			// emit the pair (sensor_id, 1)
			context.write(new Text(sensor_id), new IntWritable(1));
		}
}

protected void reduce(Text key, Iterable<IntWritable> values, Context context) {

	int numDays = 0;

	// Iterate over the set of values and sum them
	for (IntWritable value : values) {
		numDays = numDays + value.get();
	}

	context.write(new Text(key), new IntWritable(numDays));
}

/* Exercise 4 - (zoneId, list of dates) with a pm10 value above a
 specific theshold (ONE job, MANY reducers) */

protected void map(LongWritable key, Text value, Context context){
	
	String[] fields = key.toString().split(",");

	String zone = fields[0];
	String date = fields[1];
	Double PM10Level = new Double(value.toString());

	// Compare the value of PM10 with the threshold value
	if (PM10Level > PM10Threshold) {
		// emit the pair (sensor_id, 1)
		context.write(new Text(zone), new Text(date));
	}
}

protected void reduce(Text key, Iterable<Text> values, Context context) {
	
	String aboveThresholdDates = new String();

	// Iterate over the set of values and concatenate them
	for (Text date : values) {
		if (aboveThresholdDates.length() == 0)
			aboveThresholdDates = new String(date.toString());
		else
			aboveThresholdDates = aboveThresholdDates.concat("," + date.toString());
	}

	context.write(new Text(key), new Text(aboveThresholdDates));
}

/* Exercise 5 - (sensorId, average PM10 value) (ONE job, MANY reducers) */

protected void map(LongWritable key, Text value, Context context) {

	String[] fields = value.toString().split(",");
	String sensorId = fields[0];
	float PM10value = Float.parseFloat(fields[2]);

	// emit the pair (sensor_id, reading value)
	context.write(new Text(sensorId), new FloatWritable(new Float(PM10value)));
}

protected void reduce(Text key, Iterable<FloatWritable> values, Context context) {

	int count = 0;
	double sum = 0;

	// Iterate over the set of values and sum them. Count also the number of values
	for (FloatWritable value : values) {
		sum = sum + value.get();
		count = count + 1;
	}
	// Compute average value and emits pair (sensor_id, average)
	context.write(new Text(key), new FloatWritable((float) sum / count));
}

/* Exercise 6 - (sensorId, max_value=?_min_value=?) (ONE job, MANY reducers) */

protected void map(LongWritable key, Text value, Context context) {

	String[] fields = value.toString().split(",");

	// emit the pair (sensor_id, reading value)
	context.write(new Text(fields[0]), new FloatWritable(new Float(fields[2])));
}

protected void reduce(Text key, Iterable<FloatWritable> values, Context context) {
	
	double min = Double.MAX_VALUE;
	double max = Double.MIN_VALUE;

	// Iterate over the set of values and update min and max.
	for (FloatWritable value : values) {
		if (value.get() > max) 
			max = value.get();
		
		if (value.get() < min) 
			min = value.get();
	}

	// Emits pair (sensor_id, max_min)
	context.write(new Text(key), new Text("max=" + max + "_min=" + min));
}

/* Exercise 7 - (word, list of sentenceIds where the word is present) 
(ONE job, MANY reducers) */

protected void map(LongWritable key, Text value, Context context){
	
	String[] words = value.toString().split("\\s+");

	// Iterate over the set of words
	for (String word : words) {
		// Transform word case
		String cleanedWord = word.toLowerCase();

		if (cleanedWord.compareTo("and") != 0 && cleanedWord.compareTo("or") != 0
				&& cleanedWord.compareTo("not") != 0)
			// emit the pair (word, sentenceid)
			context.write(new Text(cleanedWord), new Text(key));
	}
}

protected void reduce(Text key, Iterable<Text> values, Context context){
	
	String invIndex = new String();

	// Iterate over the set of sentenceids and concatenate them
	for (Text value : values) {
		invIndex = invIndex.concat(value + ",");
	}
	context.write(key, new Text(invIndex));
}

/* Exercise 8 - (year-month, totalIncome) (year, avg monthly income)  (MANY reducers) */

//VERSION TWO JOBS

protected void map(LongWritable key, Text value, Context context) {
	
	String[] date = key.toString().split("-");

	String month = new String(date[0] + "-" + date[1]);

	// emit the pair (month, value)
	context.write(new Text(month), new DoubleWritable(Double.parseDouble(value.toString())));
}

protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
	
	double totalIncome = 0;

	// Iterate over the set of values and sum them
	for (DoubleWritable value : values) {
		totalIncome = totalIncome + value.get();
	}
	context.write(new Text(key), new DoubleWritable(totalIncome));
}

protected void map2(LongWritable key, Text value, Context context) {
	
	String[] month = key.toString().split("-");

	String year = new String(month[0]);

	// emit the pair (month, value)
	context.write(new Text(year), new DoubleWritable(Double.parseDouble(value.toString())));
}

protected void reduce2(Text key, Iterable<DoubleWritable> values, Context context) {
	
	double totalIncome = 0;
	int count = 0;

	// Iterate over the set of values and sum them
	for (DoubleWritable value : values) {
		totalIncome = totalIncome + value.get();
		count++;
	}

	context.write(new Text(key), new DoubleWritable(totalIncome / count));
}
	
//VERSION ONE JOB	
	
protected void map(LongWritable key, Text value, Context context) {	
	
	String[] date = key.toString().split("-");
	String year = date[0];
	String monthID = date[1];

	Double income = Double.parseDouble(value.toString());

	MonthIncome monthIncome = new MonthIncome();

	monthIncome.setMonthID(monthID);
	monthIncome.setIncome(income);

	// emit the pair (year, (month,income))
	context.write(new Text(year), monthIncome);
	
}
	
protected void reduce(Text key, Iterable<MonthIncome> values, Context context) {
			
	HashMap<String, Double> totalMonthIncome = new HashMap<String, Double>();

	String year = key.toString();

	double totalYearlyIncome = 0;
	int countMonths = 0;

	// Iterate over the set of values and compute
	// - the total income for each month
	// - the overall total income for this year
	for (MonthIncome value : values) {
		Double income = totalMonthIncome.get(value.getMonthID());

		if (income != null) {
			// Update the total income for this month
			totalMonthIncome.put(new String(value.getMonthID()), new Double(value.getIncome() + income));
		} else {
			// First occurrence of this monthId
			// Insert monthid - income in the hashmap
			totalMonthIncome.put(new String(value.getMonthID()), new Double(value.getIncome()));

			// Update the number of months of the current year
			countMonths++;
		}

		// Update the total income of the current year
		totalYearlyIncome = totalYearlyIncome + value.getIncome();
	}

	// Emit the pairs (year-month, total monthly income)
	for (Entry<String, Double> pair : totalMonthIncome.entrySet()) {
		context.write(new Text(year + "-" + pair.getKey()), new DoubleWritable(pair.getValue()));
	}

	// Emit the average monthly income for each year
	context.write(new Text(year), new DoubleWritable(totalYearlyIncome / countMonths));

}

public class MonthIncome implements org.apache.hadoop.io.Writable {

	private String monthID;
	private double income;
}

/* Exercise 9 - word count problem with in-mapper combiner (ONE job, MANY reducers) */
	
HashMap<String, Integer> wordsCounts;

protected void setup(Context context) {
	wordsCounts = new HashMap<String, Integer>();
}

protected void map(LongWritable key, Text value, Context context)  {

	Integer currentFreq;
	String[] words = value.toString().split("\\s+");

	// Iterate over the set of words
	for (String word : words) {
		// Transform word case
		String cleanedWord = word.toLowerCase();
		currentFreq = wordsCounts.get(cleanedWord);

		if (currentFreq == null) { // it is the first time that the mapper finds this word
			wordsCounts.put(new String(cleanedWord), new Integer(1));
		} else { // Increase the number of occurrences of the current word
			currentFreq = currentFreq + 1;
			wordsCounts.put(new String(cleanedWord), new Integer(currentFreq));
		}
	}
}

protected void cleanup(Context context){

	// Emit the set of (key, value) pairs of this mapper
	for (Entry<String, Integer> pair : wordsCounts.entrySet()) {
		context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
	}
}	

protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
	
	int occurrances = 0;

	// Iterate over the set of values and sum them 
	for (IntWritable value : values) {
		occurrances = occurrances + value.get();
	}
	context.write(key, new IntWritable(occurrances));
}
	
/* Exercise 10 - total number of record in the input file 
(ONE job, 0 reducers) -- MAP ONLY */

//in the driver
if (job.waitForCompletion(true) == true) {
	exitCode = 0;

	Counter totRecords = job.getCounters().findCounter(MY_COUNTERS.TOTAL_RECORDS);
	System.out.println("Total number of records = " + totRecords.getValue());
} else
	exitCode = 1;

protected void map(LongWritable key, Text value, Context context) {

	context.getCounter(MY_COUNTERS.TOTAL_RECORDS).increment(1);
}

/* Exercise 11 - (sensorId, avg PM10 value) suppose to have just 2 
sensors s1 and s2 (ONE job, MANY reducers) */	

HashMap<String, SumCount> statistics;

protected void setup(Context context) {
	statistics = new HashMap<String, SumCount>();
}

protected void map(LongWritable key, Text value, Context context) {

	SumCount currentStat;

	String[] fields = value.toString().split(",");
	String sensorId = fields[0];
	float measure = Float.parseFloat(fields[2]);

	currentStat = statistics.get(sensorId);

	if (currentStat == null) {
		currentStat = new SumCount();

		currentStat.setCount(1);
		currentStat.setSum(measure);

		statistics.put(new String(sensorId), currentStat);
	} else {
		currentStat.setCount(currentStat.getCount() + 1);
		currentStat.setSum(currentStat.getSum() + measure);

		statistics.put(new String(sensorId), currentStat);
	}
}

protected void cleanup(Context context){

	for (Entry<String, SumCount> pair : statistics.entrySet()) {
		context.write(new Text(pair.getKey()), pair.getValue());
	}
}

protected void reduce(Text key, Iterable<SumCount> values, Context context) {
	
	int count=0;
	float sum=0;
	
	// Iterate over the set of values and sum them.
	// Count also the number of values
	for (SumCount value : values) {
		sum=sum+value.getSum();
		
		count=count+value.getCount();
	}

	SumCount sumCountPerSensor= new SumCount();
	sumCountPerSensor.setCount(count);
	sumCountPerSensor.setSum(sum);
	

	// Emits pair (sensor_id, sum-count = average)
	context.write(new Text(key), sumCountPerSensor);
}

public class SumCount implements org.apache.hadoop.io.Writable {
	private float sum = 0;
	private int count = 0;
}

/* Exercise 12 - ((sensorId,date), PM10 value) where the value is below a 
threshold passed as parameter (ONE job, 0 reducers) -- MAP ONLY*/	

float threshold;

protected void setup(Context context) {
	// I retrieve the value of the threshold only one time for each mapper
	threshold = Float.parseFloat(context.getConfiguration().get("maxThreshold"));
}

protected void map(Text key, Text value, Context context){

	float measure = Float.parseFloat(value.toString());

	// Filter the reading based on the value of threshold
	if (measure < threshold) {
		context.write(new Text(key), new FloatWritable(measure));
	}
}

/* Exercise 13 - (date, income) TOP 1 most profitable day 
(ONE job, 1 reducer) */	

private DateIncome top1;

protected void setup(Context context) {
	// for each mapper, top1 i used to store the information about the top1
	// date-income of the subset of lines analyzed by the mapper
	top1 = new DateIncome();
	top1.setIncome(Float.MIN_VALUE);
	top1.setDate(null);
}

protected void map(Text key, Text value, Context context) {

	String date = new String(key.toString());

	float dailyIncome = Float.parseFloat(value.toString());

	// Check if the current income is the top 1 income among the ones
	// analyzed by this
	// mapper. In case of tie (same income value) the earliest date is
	// considered.
	if (dailyIncome > top1.getIncome() || 
		(dailyIncome == top1.getIncome() && date.compareTo(top1.getDate()) < 0)) {
		// The current line is the current top 1 income value. Store date
		// and income in top1
		top1 = new DateIncome();
		top1.setDate(date);
		top1.setIncome(dailyIncome);
	}
}

protected void cleanup(Context context) {
	// Emit the top1 date and income related to this mapper
	context.write(NullWritable.get(), top1);
}

protected void reduce(NullWritable key, Iterable<DateIncome> values, Context context){

	String date;
	float dailyIncome;

	DateIncome globalTop1 = new DateIncome();
	globalTop1.setIncome(Float.MIN_VALUE);
	globalTop1.setDate(null);

	// Iterate over the set of values and select the top 1 income and
	// the related date
	for (DateIncome value : values) {

		date = value.getDate();
		dailyIncome = value.getIncome();

		if (dailyIncome > globalTop1.getIncome() || 
			(dailyIncome == globalTop1.getIncome() && 
			date.compareTo(globalTop1.getDate()) < 0)) {
			// The current line is the current top 1 income value. Store
			// date and income in globalTop1
			globalTop1 = new DateIncome();
			globalTop1.setDate(date);
			globalTop1.setIncome(dailyIncome);
		}
	}
	// Emit pair (date, income) associated with top 1 income
	context.write(new Text(globalTop1.getDate()), 
					new FloatWritable(globalTop1.getIncome()));
}

public class DateIncome implements org.apache.hadoop.io.Writable {

	private String date;
	private float income;
}

/* Exercise 14 - list of distinct words in an input file 
(ONE job, MANY reducer) */	

protected void map(Text key, Text value, Context context) {

	String[] words = value.toString().split("\\s+");
            
	// Iterate over the set of words
	for(String word : words) {
		// Transform word case
		String cleanedWord = word.toLowerCase();
		
		// emit the pair (word, null)
		context.write(new Text(cleanedWord), NullWritable.get());
	}
}

protected void reduce(Text key, Iterable<NullWritable> values, Context context){
	
	context.write(key, NullWritable.get());
}
//COMBINER
protected void reduce(Text key, Iterable<NullWritable> values, Context context){

	context.write(key, NullWritable.get());
}
	
/* Exercise 15 - list of distinct words in an input file associated 
with a unique identifier (word, number) --> increasing starting from 1 
(ONE job, 1 reducer) */	

protected void map(Text key, Text value, Context context) {

	String[] words = value.toString().split("\\s+");
				
	// Iterate over the set of words
	for(String word : words) {
		// Transform word case
		String cleanedWord = word.toLowerCase();
		
		// emit the pair (word, null)
		context.write(new Text(cleanedWord), NullWritable.get());
	}
}

int wordId;

protected void setup(Context context) {
	// Initialize the variable that is used to remember how many words have
	// been already mapped to an integer (i.e., it stores also the last
	// integer value mapped with a word)
	wordId = 0;
}

@Override
protected void reduce(Text key,Iterable<NullWritable> values, Context context){

	// Emit the current word associated with the next available integer
	wordId = wordId + 1;
	context.write(key, new IntWritable(wordId));
}
//COMBINER
protected void reduce(Text key, Iterable<NullWritable> values, Context context){

	context.write(key, NullWritable.get());
}


/* Exercise 17 - given two input file compute the max temperature for each date 
(date, temperature)  (ONE job, MANY reducers) */	

protected void map(LongWritable key, Text value, Context context){

	String[] fields = value.toString().split(",");
	
	String date=fields[1];
	float temperature=Float.parseFloat(fields[3]);
	
	context.write(new Text(date), new FloatWritable(temperature));
}	
//mapper for second file 
protected void map(LongWritable key, Text value, Context context){

	String[] fields = value.toString().split(",");
	
	String date=fields[0];
	float temperature=Float.parseFloat(fields[2]);
	
	context.write(new Text(date), new FloatWritable(temperature));
}	

protected void reduce(Text key, Iterable<FloatWritable> values, Context context){
 
	float max=Float.MIN_VALUE;
	
	// Iterate over the set of values and find the maximum value
	for (FloatWritable value : values) {
		if (value.get()>max)
			max=value.get();
	}
	// Emit pair (date, maximum temperature)
	context.write(new Text(key), new FloatWritable(max));
}

/* Exercise 18 - filter input file, keep only ones with temeperature > threshold
  (ONE job, 0 reducer) --> MAP ONLY*/
  
 protected void map(LongWritable key, Text value, Context context){
	 
	String[] fields = value.toString().split(",");

	float temperature = Float.parseFloat(fields[3]);

	if (temperature > 30.0)
		context.write(value, NullWritable.get());
}

/* Exercise 20 - filter input file, put lines with temeperature >= threshold
 in one file with prefix 'high-temp' and others with temperature <= threshold
 in another file with prefix 'low-temp'  (ONE job, 0 reducer) --> MAP ONLY*/
 
 //in the driver

MultipleOutputs.addNamedOutput(job, "hightemp", TextOutputFormat.class, 
										Text.class, NullWritable.class);
MultipleOutputs.addNamedOutput(job, "normaltemp", TextOutputFormat.class, 
										Text.class, NullWritable.class);
										
// Define a MultiOutputs object
private MultipleOutputs<Text, NullWritable> mos = null;

protected void setup(Context context){
	// Create a new MultiOuputs using the context object
	mos = new MultipleOutputs<Text, NullWritable>(context);
}

protected void map(LongWritable key, Text value, Context context){

		String[] fields = value.toString().split(",");
		
		float temperature=Float.parseFloat(fields[3]);
		
		if (temperature > 30.0)
			mos.write("hightemp", value, NullWritable.get());
		else
			mos.write("normaltemp", value, NullWritable.get());
			
}

protected void cleanup(Context context) {
	// Close the MultiOutputs
	// If you do not close the MultiOutputs object the content of the output
	// files will not be correct
	mos.close();
}


/* Exercise 21 - two input files: 
	- sentences
	- words to delete from first file
	result is the first file without words from the second one 
(ONE job, 0 reducer) --> MAP ONLY*/


//in the driver

// Add hdfs file stopwords.txt in the distributed cache
// stopwords.txt can now be read by all mappers of this application
// independently of the nodes of the cluser used to run the application
job.addCacheFile(new Path("stopwords.txt").toUri());

private ArrayList<String> stopWords;

protected void setup(Context context){
	
	String nextLine;

	stopWords=new ArrayList<String>();
	// Open the stopword file (that is shared by means of the distributed 
	// cache mechanism) 
	Path[] PathsCachedFiles = context.getLocalCacheFiles();	
	
	// This application has one single single cached file. 
	// Its path is URIsCachedFiles[0] 
	BufferedReader fileStopWords = new BufferedReader
				(new FileReader(new File(PathsCachedFiles[0].toString())));

	// Each line of the file contains one stopword 
	// The stopwords are stored in the stopWords list
	while ((nextLine = fileStopWords.readLine()) != null) {
		stopWords.add(nextLine);
	}

	fileStopWords.close();
}

protected void map(LongWritable key, Text value, Context context){

		boolean stopword;
		String[] words = value.toString().split("\\s+");
		
		// Remove stopwords from the current sentence
		String sentenceWithoutStopwords=new String("");
		// Iterate over the set of words
		for(String word : words) {
			
			// if the current word is in the stopWords 
			// list it means it is a stopword 
			if (stopWords.contains(word)==true)
				stopword=true;
			else
				stopword=false;
				
			// If the word is a stopword do not consider it
			// Otherwise attach it at the end of sentenceWithoutStopwords
			if (stopword==false)
				sentenceWithoutStopwords=sentenceWithoutStopwords.concat(word+" ");
			
		}
		
		// emit the pair (null, sentenceWithoutStopwords)
		context.write(NullWritable.get(), new Text(sentenceWithoutStopwords));
}

/* Exercise 22 - given a file with (user, user) meaning they are friends, 
print list of friends of a specified (parameter) user
(ONE job, 1 reducer) */


 protected void map(LongWritable key, Text value, Context context){

	String specifiedUser = context.getConfiguration().get("username");

	// Extract username1 and username2
	String[] users = value.toString().split(",");
	
	// Check if one of the users is specifiedUser
	if (specifiedUser.compareTo(users[0])==0)
		// emit the pair (null, users[1])
		context.write(NullWritable.get(), new Text(users[1]));

	if (specifiedUser.compareTo(users[1])==0)
		// emit the pair (null, users[0])
		context.write(NullWritable.get(), new Text(users[0]));    
}

protected void reduce(NullWritable key, Iterable<Text> values, Context context) {

	String listOfFriends=new String("");
	
	// Iterate over the set of values and concatenate them 
	for (Text value : values) {
		listOfFriends=listOfFriends.concat(value.toString()+" ");
	}
	
	context.write(new Text(listOfFriends), NullWritable.get());
}


/* Exercise 23 - given a file with (user, user) meaning they are friends, 
print list of friends and potential friends of a specified (parameter) user
(ONE job, 1 reducer) */

//VERSION ONE JOB
String specifiedUser;

protected void setup(Context context) {
	// Retrieve the information about the user of interest
	specifiedUser = context.getConfiguration().get("username");
}

protected void map(LongWritable key, Text value, Context context){

	// Extract username1 and username2
	String[] users = value.toString().split(",");

	// Emit two key-value pairs
	// (username1,username2)
	// (username2,username1)
	// Do not emit pair with key=user of interest. It is not useful
	if (specifiedUser.compareTo(users[0]) != 0)
		context.write(new Text(users[0]), new Text(users[1]));

	if (specifiedUser.compareTo(users[1]) != 0)
		context.write(new Text(users[1]), new Text(users[0]));
}

HashSet<String> finalListPotentialFriends;
String specifiedUser;

protected void setup(Context context){
	// Retrieve the information about the user of interest
	specifiedUser = context.getConfiguration().get("username");

	// Instantiate the local variable that is used to store the complete
	// list of potential friends
	finalListPotentialFriends = new HashSet<String>();
}

protected void reduce(Text key, Iterable<Text> values, Context context){

	boolean containsSpecifiedUser;

	// Partial list of potential friends
	HashSet<String> partialListOfPotentialFriends = new HashSet<String>();

	// Key contains one username.
	// If values contains the specified user it means that the specified
	// user and the other users in values have user "key" in common.
	// Hence, the users in values are potential friends
	containsSpecifiedUser = false;

	for (Text value : values) {
		if (specifiedUser.compareTo(value.toString()) == 0)
			containsSpecifiedUser = true;
		else {
			// Store the list of users for a potential "second iteration"
			partialListOfPotentialFriends.add(value.toString());
		}
	}

	// If containsSpecifiedUser is true it means that
	// partialListOfPotentialFriends
	// contains potential friends of the specified user
	// It is useful if and only if partialListOfPotentialFriends is not
	// empty (i.e., if values
	// contains the selected user and also another one)

	if (containsSpecifiedUser == true && 
		partialListOfPotentialFriends.size() > 0) {
		// Extract the list of potential users for
		// partialListOfPotentialFriends

		for (String user : partialListOfPotentialFriends) {
			// If the user is new then it is inserted in the set
			// Otherwise, if it is already in the set, it is ignored
			finalListPotentialFriends.add(user);
		}
	}
}

protected void cleanup(Context context){
	// Concatenate the users stored in finalListPotentialFriends
	// to generate the final result
	String globalPotFriends = new String("");

	for (String potFriend : finalListPotentialFriends) {
		globalPotFriends = globalPotFriends.concat(potFriend + " ");
	}

	context.write(new Text(globalPotFriends), NullWritable.get());
}


//version of 2 jobs (MANY reducers, 1 reducer) -- first version

String specifiedUser;

protected void setup(Context context) {
	// Retrieve the information about the user of interest
	specifiedUser = context.getConfiguration().get("username");
}

protected void map(LongWritable key, Text value, Context context){

	// Extract username1 and username2
	String[] users = value.toString().split(",");

	// Emit two key-value pairs
	// (username1,username2)
	// (username2,username1)
	// Do not emit pair with key=user of interest. It is not useful
	if (specifiedUser.compareTo(users[0]) != 0)
		context.write(new Text(users[0]), new Text(users[1]));

	if (specifiedUser.compareTo(users[1]) != 0)
		context.write(new Text(users[1]), new Text(users[0]));
}

HashSet<String> finalListPotentialFriends;
String specifiedUser;

protected void setup(Context context){
	// Retrieve the information about the user of interest
	specifiedUser = context.getConfiguration().get("username");

	// Instantiate the local variable that is used to store the complete
	// list of potential friends
	finalListPotentialFriends = new HashSet<String>();
}

protected void reduce(Text key, Iterable<Text> values, Context context){

	boolean containsSpecifiedUser;

	// Partial list of potential friends
	HashSet<String> partialListOfPotentialFriends = new HashSet<String>();

	// Key contains one username.
	// If values contains the specified user it means that the specified
	// user and the other users in values have user "key" in common.
	// Hence, the users in values are potential friends
	containsSpecifiedUser = false;

	for (Text value : values) {
		if (specifiedUser.compareTo(value.toString()) == 0)
			containsSpecifiedUser = true;
		else {
			// Store the list of users for a potential "second iteration"
			partialListOfPotentialFriends.add(value.toString());
		}
	}

	// If containsSpecifiedUser is true it means that
	// partialListOfPotentialFriends
	// contains potential friends of the specified user
	// It is useful if and only if partialListOfPotentialFriends is not
	// empty (i.e., if values
	// contains the selected user and also another one)

	if (containsSpecifiedUser == true && 
			partialListOfPotentialFriends.size() > 0) {
		// Extract the list of potential users for
		// partialListOfPotentialFriends

		for (String user : partialListOfPotentialFriends) {
			// If the user is new then it is inserted in the set
			// Otherwise, if it is already in the set, it is ignored
			finalListPotentialFriends.add(user);
		}
	}
}

protected void cleanup(Context context) {
	// Emit the partial list of potential friends of the user of interest.
	// In this first job I do not need to emit one single line.
	// Emit one line for each potential friend

	for (String potFriend : finalListPotentialFriends) {
		context.write(new Text(potFriend), NullWritable.get());
	}
}

protected void map(LongWritable key, Text value, Context context){

	// It simply reads the input data and emit a copy of it to the single invocation
	// of the reducer that is used to compute the global final result
	// Each emitted pair contains a local subset of the potential friends of the
	// user of interest
	context.write(NullWritable.get(), new Text(value.toString()));
}

protected void reduce(NullWritable key, Iterable<Text> values, Context context){

	HashSet<String> potentialFriends = new HashSet<String>();

	for (Text value : values) {
		// Each value is one potential friend
		potentialFriends.add(value.toString());
	}

	String globalPotFriends;

	// Concatenate the users in potentialFriends
	globalPotFriends = new String("");

	for (String potFriend : potentialFriends) {
		globalPotFriends = globalPotFriends.concat(potFriend + " ");
	}

	context.write(new Text(globalPotFriends), NullWritable.get());
}

//version of 2 jobs (1 reducer, 1 reducer) -- second version

String specifiedUser;

protected void setup(Context context) {
	specifiedUser = context.getConfiguration().get("username");
}

protected void map(LongWritable key, Text value, Context context){

	// Extract username1 and username2
	String[] users = value.toString().split(",");

	// Check if one of the users is specifiedUser
	if (specifiedUser.compareTo(users[0]) == 0) {
		// emit the pair (null, users[1])
		context.write(NullWritable.get(), new Text(users[1]));
	}

	if (specifiedUser.compareTo(users[1]) == 0) {
		// emit the pair (null, users[0])
		context.write(NullWritable.get(), new Text(users[0]));
	}
}

protected void reduce(NullWritable key, Iterable<Text> values, Context context){

	// Iterate over the set of values and emit one line for each of friend of the
	// user of interest
	for (Text value : values) {
		context.write(new Text(value.toString()), NullWritable.get());
	}
}

String specifiedUser;
ArrayList<String> friends;

protected void setup(Context context){
	
	String line;

	// Store the information about the user of interest
	specifiedUser = context.getConfiguration().get("username");

	// Store in the ArraList friends the list of friends available in the
	// shared file
	friends = new ArrayList<String>();

	URI[] CachedFiles = context.getCacheFiles();

	// This application has one single single cached file.
	// Its path is CachedFiles[0].getPath()
	BufferedReader fileFriends = new BufferedReader(
			new FileReader(new File(CachedFiles[0].getPath())));

	// There is one friend per line
	while ((line = fileFriends.readLine()) != null) {
		friends.add(line);
	}

	fileFriends.close();
}

protected void map(LongWritable key, Text value, Context context){

	// Extract username1 and username2
	String[] users = value.toString().split(",");

	// Check if one of the two users is friend of the user of interest.
	// If it is true, the the other user of the current pair is a potential friend
	// of the user of interest

	if (friends.contains(users[0]) == true && 
			users[1].compareTo(specifiedUser) != 0) {
		// users[0] is a friend of specifiedUser
		// users[1] is a potential friend of specifiedUser
		// emit the pair (null, users[1])
		context.write(NullWritable.get(), new Text(users[1]));
	}

	if (friends.contains(users[1]) == true && 
			users[0].compareTo(specifiedUser) != 0) {
		// users[1] is a friend of specifiedUser
		// users[0] is a potential friend of specifiedUser
		// emit the pair (null, users[0])
		context.write(NullWritable.get(), new Text(users[0]));
	}
}

protected void reduce(NullWritable key, Iterable<Text> values, Context context){

	ArrayList<String> potFriends = new ArrayList<String>();
	String listOfPotFriends = new String("");

	// Iterate over the set of values and include them in the ArrayList of
	// potential friends
	for (Text value : values) {
		if (potFriends.contains(value.toString()) == false)
			potFriends.add(value.toString());
	}

	// Concatenate the list of potential friends
	for (String potFriend : potFriends) {
		listOfPotFriends = listOfPotFriends.concat(potFriend + " ");
	}

	// Emit the list of potential friends (in one single line)
	context.write(new Text(listOfPotFriends), NullWritable.get());
}


/* Exercise 24 - given a file with (user, user) meaning they are friends, 
print for each user the list of his friends
(ONE job, MANY reducers) */

 protected void map(LongWritable key, Text value, Context context){

	// Extract username1 and username2
	String[] users = value.toString().split(",");
	
	// Emit two key-value pairs
	// (username1,username2)
	// (username2,username1)
	context.write(new Text(users[0]), new Text(users[1]));
	context.write(new Text(users[1]), new Text(users[0]));
}

protected void reduce(Text key, Iterable<Text> values, Context context){

	String listOfFriends=new String("");
	
	// Key contains one userame.
	// Iterate over the set of values and concatenate them to build the 
	// list of friend of the username specified in key.
	for (Text value : values) {
		listOfFriends=listOfFriends.concat(value.toString()+" ");
	}
	
	context.write(new Text(key+":"), new Text(listOfFriends));
}

/* Exercise 25 - given a file with (user, user) meaning they are friends, 
print for each user the list of his potential friends
(TWO job, MANY reducers) */

protected void map(LongWritable key, Text value, Context context){

	// Extract username1 and username2
	String[] users = value.toString().split(",");
	
	// Emit two key-value pairs
	// (username1,username2)
	// (username2,username1)
	context.write(new Text(users[0]), new Text(users[1]));
	context.write(new Text(users[1]), new Text(users[0]));
}

 protected void reduce(Text key, Iterable<Text> values, Context context){

	HashSet<String> users;

	// Each user in values is potential friend of the other users in values
	// because they have the user "key" in common.
	// Hence, the users in values are potential friends of each others.    	
	// Since it is not possible to iterate more than one time on values
	// we need to create a local copy of it. However, the 
	// size of values is at most equal to the friend of user "key". Hence,
	// it is a small list

	users=new HashSet<String>();

	for (Text value : values) {
		users.add(value.toString());
	}

	// Compute the list of potential friends for each user in users

	for (String currentUser: users)
	{
		String listOfPotentialFriends=new String("");
	
		for (String potFriend: users) 
		{	// If potFriend is not currentUser then include him/her in the 
			// potential friends of currentUser
			if (currentUser.compareTo(potFriend)!=0)
				listOfPotentialFriends=listOfPotentialFriends.concat(potFriend+" ");
		}
		
		// Check if currentUser has at least one friend
		if (listOfPotentialFriends.compareTo("")!=0)
			context.write(new Text(currentUser), new Text(listOfPotentialFriends));
	}
}

 protected void map(Text key, Text value, Context context){

	// Emit one key-value pair of each user in value.
	// Key is equal to the key of the input key-value pair
	String[] users = value.toString().split(" ");
	
	for (String user: users)
	{
		context.write(new Text(key.toString()), new Text(user));
	}
}

protected void reduce(Text key, Iterable<Text> values, Context context){

	String listOfPotentialFriends;
	HashSet<String> potentialFriends;
	
	potentialFriends=new HashSet<String>();
	
	// Iterate over the values and include the users in the final set
	for (Text user: values)
	{
		// If the user is new then it is inserted in the set
		// Otherwise, it is already in the set, it is ignored
		potentialFriends.add(user.toString());
	}

	listOfPotentialFriends=new String("");
	for (String user: potentialFriends)
	{
		listOfPotentialFriends=listOfPotentialFriends.concat(user+" ");
	}

	context.write(new Text(key), new Text(listOfPotentialFriends));
}

/* Exercise 26 - given two input files:
	- a textual file
	- a small dictionary (number, word) 
	result = first file but with corresponding numbers
(ONE job, 0 reducer) -- MAP ONLY*/

//in the driver
// Add hdfs file dictionary.txt in the distributed cache
// stopwords.txt can now be read by all mappers of this application
// independently of the nodes of the cluser used to run the application
job.addCacheFile(new Path("dictionary.txt").toUri());

private HashMap<String,Integer> dictionary;
	
protected void setup(Context context){
	
	String line;
	String word;
	Integer intValue;

	dictionary=new HashMap<String, Integer>();
	// Open the dictionary file (that is shared by means of the distributed 
	// cache mechanism) 
	
	Path[] PathsCachedFiles = context.getLocalCacheFiles();	
	
	// This application has one single single cached file. 
	// Its path is URIsCachedFiles[0] 
	BufferedReader fileStopWords = new BufferedReader
			(new FileReader(
					new File(PathsCachedFiles[0].toString())));
					
	// Each line of the file contains one mapping 
	// word integer 
	// The mapping is stored in the dictionary HashMap variable
	while ((line = fileStopWords.readLine()) != null) {
		
		// record[0] = integer value associated with the word
		// record[1] = word
		String[] record=line.split("\t");
		intValue=new Integer(record[0]);
		word=record[1];
		
		dictionary.put(word,intValue);
	}

	fileStopWords.close();
}

protected void map(LongWritable key, Text value, Context context){

	String convertedString;
	Integer intValue;

	String[] words = value.toString().split("\\s+");

	// Convert words to integers
	convertedString=new String("");
	
	// Iterate over the set of words
	for(String word : words) {
	
		// Retrieve the integer associated with the current word
		intValue=dictionary.get(word.toUpperCase());
		
		convertedString=convertedString.concat(intValue+" ");
	}
	// emit the pair (null, sentenceWithoutStopwords)
	context.write(NullWritable.get(), new Text(convertedString));
}


/* Exercise 27 - given two input files:
	- a structured file with users infos
	- a small file with a set of business rules 
	result = info for each user + category based on rules
	there can be at most one category for each user, if none put unknown
(ONE job, 0 reducer) -- MAP ONLY*/

//in the driver
// Add hdfs file businessrules.txt in the distributed cache
job.addCacheFile(new Path("businessrules.txt").toUri());

private ArrayList<String> rules;

protected void setup(Context context){
	String nextLine;

	rules=new ArrayList<String>();
	// Open the business rules file (that is shared by means of the distributed 
	// cache mechanism) 
			
	Path[] PathsCachedFiles = context.getLocalCacheFiles();	
	
	// This application has one single single cached file. 
	// Its path is URIsCachedFiles[0] 
	BufferedReader rulesFile = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));

	// Each line of the file contains one rule 
	while ((nextLine = rulesFile.readLine()) != null) {
		rules.add(nextLine);
	}
	rulesFile.close();
}

private String applyBusinessRule(String gender, String year){
	
	String category=new String("Unknown");

	// Iterate over the rules
	for (String rule: rules)
	{
		// Gender=<value> and DateOfBirth=<value> -> Category
		String[] ruleParts=rule.split(" ");
		// ruleParts[0] = Gender=<value>
		// ruleParts[2] = DateOfBirth=<value>
		// ruleParts[4] = category
		// Check if the current rule is satisfied by the current user 
		if (ruleParts[0].compareTo("Gender="+gender)==0 && 
				ruleParts[2].compareTo("YearOfBirth="+year)==0)
			category=ruleParts[4];
	}
	
	return category;
}

protected void map(LongWritable key, Text value, Context context){

		String category;
		// Split each record in fields
		// UserId,Name,Surname,Gender,YearOfBirth,City,Education
		String[] fields = value.toString().split(",");

		category=applyBusinessRule(fields[3],fields[4]);
		  
		// emit the pair (null, record+category)
		context.write(NullWritable.get(), new Text(value.toString()+","+category));
}


/* Exercise 28 - given two input files:
	- a set of questions
	- a set of answers
	result = (question, answer) with a specific format
(ONE job, MANY reducers)*/

//in the driver
// Set two input paths and two mapper classes
MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class,
				MapperType1BigData.class);
MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, 
				MapperType2BigData.class);

//mapper 1
protected void map(LongWritable key, Text value, Context context){

	// Record format
	// QuestionId,Timestamp,TextOfTheQuestion
	String[] fields=value.toString().split(",");
	
	String questionId=fields[0];
	String questionText=fields[2];
	
	// Key = questionId
	// Value = Q:+questionId,questionText
	// Q: is used to specify that this pair has been emitted by
	// analyzing a question
	context.write(new Text(questionId), new Text("Q:"+questionId+","+questionText));
}

//mapper 2
protected void map(LongWritable key, Text value, Context context){
	
	// Record format
	// AnswerId,QuestionId,Timestamp,TextOfTheAnswer
	String[] fields=value.toString().split(",");

	String answerId=fields[0];
	String answerText=fields[3];
	String questionId=fields[1];

	// Key = questionId
	// Value = A:+answerId,answerText
	// A: is used to specify that this pair has been emitted by
	// analyzing an answer
	context.write(new Text(questionId), new Text("A:"+answerId+","+answerText));
}

protected void reduce(Text key, Iterable<Text> values, Context context){

	String record;
	ArrayList<String> answers=new ArrayList<String>();
	String question=null;

	// Iterate over the set of values and store the answer records in 
	// answers and the question record in question
	for (Text value : values) {
		
		String table_record=value.toString();
		
		if (table_record.startsWith("Q:")==true)
		{	// This is the question record
			record=table_record.replaceFirst("Q:", "");
			question=record;
		}
		else
		{	// This is an answer record
			record=table_record.replaceFirst("A:", "");
			answers.add(record);
		}
	}

	// Emit one pair (question, answer) for each answer
	for (String answer:answers)
	{
		context.write(NullWritable.get(), new Text(question+","+answer));
	}
}


/* Exercise 29 - given two input files:
	- a large file containing a set of records
	- a large textual file containing pairs (userId, movie genre)
	result = one record for each user that likes commedia and 
	adventures. (gender, year of birth) 
	DO NOT eliminate duplicates
(ONE job, MANY reducers)*/

// Set two input paths and two mapper classes
MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, 
					MapperType1BigData.class);
MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, 
					MapperType2BigData.class);
					
 protected void map(LongWritable key, Text value, Context context){

	// Record format
	// UserId,Name,Surname,Gender,YearOfBirth,City,Education
	String[] fields=value.toString().split(",");
	
	String userId=fields[0];
	String gender=fields[3];
	String yearOfBirth=fields[4];
	
	// Key = userId
	// Value = U:+gender,yearOfBirth
	// U: is used to specify that this pair has been emitted by
	// analyzing the user file
	context.write(new Text(userId), new Text("U:"+gender+","+yearOfBirth));
}

protected void map(LongWritable key, Text value, Context context){
    		
	// Record format
	// UserId,MovieGenre
	String[] fields=value.toString().split(",");

	String userId=fields[0];
	String genre=fields[1];

	// Key = userId
	// Value = L
	// L: is used to specify that this pair has been emitted by
	// analyzing the likes file
	// Emit the pair if and only if the genre is Commedia or Adventure
	if (genre.compareTo("Commedia")==0 || genre.compareTo("Adventure")==0)
	{
		context.write(new Text(userId), new Text("L"));
	}
}

protected void reduce(Text key, Iterable<Text> values, Context context){

	int numElements;
	String userData = null;

	// Iterate over the set of values and check if
	// 1) there are three elements (one related do the users table and two
	// related to the like table
	// 2) store the information about the "profile/user" element

	numElements = 0;
	for (Text value : values) {

		String table_record = value.toString();

		numElements++;

		if (table_record.startsWith("U") == true) {
			// This is the user data record
			userData = table_record.replaceFirst("U:", "");
		}
	}

	// Emit a pair (null,user data) if the number of elements is equal to 3
	// (2 likes and 1 user data record)
	if (numElements == 3) {
		context.write(NullWritable.get(), new Text(userData));
	}
}

//ALTERNATIVE REDUCER

protected void reduce(Text key, Iterable<Text> values, Context context){

	boolean commedia;
	boolean adventure;
	String userData = null;

	// Iterate over the set of values and check if
	// both commedia and adventure are present in the list of
	// genres liked by the current user (key=userId)
	commedia = false;
	adventure = false;

	for (Text value : values) {

		String table_record = value.toString();

		if (table_record.startsWith("L:") == true) { // This is a like
														// record
			if (table_record.compareTo("L:Commedia") == 0)
				commedia = true;
			else if (table_record.compareTo("L:Adventure") == 0)
				adventure = true;
		} else {
			// This is the user data record
			userData = table_record.replaceFirst("U:", "");
		}
	}

	// Emit a pair (null,user data) if the user likes both
	// Commedia and Adventure movies
	if (commedia == true && adventure == true) {
		context.write(NullWritable.get(), new Text(userData));
	}
}