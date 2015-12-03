#Spark Assignment

Authors : 
Suzanne GARCIA and Maxime VIDAL

## Part 1 RDD
For this part we choose to use a class : 
Crimes(cdatetime:String,address:String,district:String,beat:String,grid:String,crimedescr:String,code:String,latitude:String,longitude:String)
and fill it with the csv file using a split for each line. 

### 1) What is the crime that happens the most in Sacramento ? 
We map the class to create a pair (crimedescription, 1)
We use a reduceByKey to compute number of crimes by crime type 

### 2) Give the 3 days with the highest crime count

### 3) Calculate the average of each crime per day

