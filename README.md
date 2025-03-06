# NYC_TAXI_PIPELINE

**Introduction**

In an era when there is a constant increase in data, data storage has become vital in any company and the ability to utilise the data to discover insight used in making business decision this project shines, the goal of this project is to tackle situation where companies have large dataset and are unable to convert them into actionable insights. This project take a work flow of extracting the data from a companies website and creating a connected with an Open-source GUI to manage the companies database. 

**Data Preparation:**

In other to achieve this we first need to understand the step needed to take
1. A new directory is needed to ensure all the files created will be safe in a single file and for that GIT BASH was used
2. A docker file which handled the building of the images.
3. The python script that will perform the entire ETL process and a PG network to establish a connection between the database and GUI tool

**Data Processing:**

Going into this project i knew it wasnt going to be an easy work flow, from the get go i was already facing challenges but they werent something i wasnt ready for. Under is a list of the challenges and how i was able to overcome them

 
**Data Cleaning:**

As the process of ingestion began, three crucial challenges were uncovered as the underwent the work flow process:
1.	The data was saved in a parquet format and for us to achieve our goal a csv file is required. To achieve this we needed to download the orignal file first afterwards we write a script which will convert the parquet file to a csv doc.
2.	The second cleaning was in found in the metadata of the datetime column which was originally in real data type which was converted to a datetime data type, this was achieved after the importation of sqlalchemy.
3.	The thrid challenge was creating a link between the data in the database and a gui which will be used to query the dataset using sql. To achieve this PGnetwork was used this provided a connection between the database and our database manager which is PGadmin

**Data Analysis and Investment Insights:**

This project is meticulously designed to offer indispensable insights for investors navigating the dynamic landscape of the entertainment sector. It addresses pivotal questions that are paramount for strategic decision-making in the realm of entertainment investments. The key inquiries encompass:
1. Total Monthly Sales of the Year:
   - Uncover the competitive financial transcation of each month . Gain clarity on which month is their highest paying and the reason behind it.
2. Highest the top Transcation made:
   - Identify the top destination with the highest payout made that year, its relationship to destinations with the highest request and how much impact the destinations with the highest payout affect the Grand Total transcations made that year.
3. Most Common destination:
   - Discover which destination has the highest request by passengers and how much impact it has on the overall transcation for that year.
These critical inquiries serve as the foundation for informed decision-making in the entertainment sector. Through comprehensive data visualizations derived from thorough dataset analysis, this project illuminates the path for investors seeking strategic and lucrative opportunities in the ever-evolving world of entertainment.

 
