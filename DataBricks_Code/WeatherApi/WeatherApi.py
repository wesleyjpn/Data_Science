# Databricks notebook source
# MAGIC %md #some general functions could be saved into another functions notebook for eg

# COMMAND ----------

def get_param(param):
    """ 
    Collect parameters passed to script

    Parameters
    ----------
        param : str
            name of the param to collect

    Returns
    -------
        return the parameter value as string
    """
    dbutils.widgets.text(param, "", "")
    return dbutils.widgets.get(param)
  
def generate_timestamp(date_param='2023-01-01', number_days=5):
  """ 
    Collect timestamp hour by hour ( in 24 hours ) from last number_days from date_param day recursive

    Parameters
    ----------
        date_param : YYYY-MM-DD
        number_days : int ( how many days run as recursive 5 latest day as default)

    Returns
    -------
        return timestamp range hour by hour in between date_param to previous number_days
    """
  
  for hour in range(24*number_days):
    yield datetime.strptime(date_param, '%Y-%m-%d') + timedelta(hours=hour)



    
from datetime import datetime
class LoggingConsole:
    """ 
    Class responsible to create a logging pattern for jobs execution.
    
    Attributes
    ----------
    component : str
        The name of the component that will use the logs.
    additional_infos : dict
        Additional informations to compose the log message,
        only the values in the dict will be used in the log.
        For example {'country': 'country'}
    """
    def __init__(self, component:str, additional_infos:dict=None):
        self.component = component
        self.additional_infos = additional_infos
        self.start_execution_time = datetime.utcnow()

    def prepare_additional_info_string(self):
        
        if self.additional_infos is None:
            return ''
        
        keys_values = self.additional_infos.items()
        
        additional_infos_list = [f'{str(value)}' for key, value in keys_values]
        
        additional_infos_str = ' '.join(additional_infos_list)
        
        return additional_infos_str
  
    def log_info(self, message):  
        """
        Prints a log information message with the following structure:
        <yyyy-mm-dd> - <h.mm.ss>.<ms> - <component name> <additional_infos_str> - <message>
        """   
        additional_infos_str = self.prepare_additional_info_string()
        print(f''' {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} - {self.component} {additional_infos_str} - {message}''')

    def log_error(self, message):
        """
        Prints an error information message with the following structure:
        <yyyy-mm-dd> - <h.mm.ss>.<ms> [ERROR] - <component name> <additional_infos_str> - Error: <message>
        """   
        additional_infos_str = self.prepare_additional_info_string()

        print(f''' {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} [ERROR] - {self.component} {additional_infos_str} - Error: {message} ''')

        
    def log_exception(self, message):
        """
        Prints an exception information message with the following structure:
        <yyyy-mm-dd> - <h.mm.ss>.<ms>  [EXCEPTION] - <component name> <additional_infos_str> - An Exception Occurred: <message>
        """   
        additional_infos_str = self.prepare_additional_info_string()

        print(f''' {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} [EXCEPTION] - {self.component} {additional_infos_str} - An Exception Occurred: {message} ''')

     
  

# COMMAND ----------

# MAGIC %md # Build a repository of data where we will keep the data extracted from the API. This repository should only have deduplicated data. Idempotency should also be guaranteed on the repository

# COMMAND ----------

# # Create Workflow in Python according to the following requirements:
# # Extract the last 5 days of data from the free API: https://openweathermap.org (Historical weather data) from 10 different locations to choose by the candidate.
# # Build a repository of data where we will keep the data extracted from the API. This repository should only have deduplicated data. Idempotency should also be guaranteed on the repository
# # Build another repository of data that will contain the results of the following calculations from the data stored in step 2.
# # A dataset containing the location, date and temperature of the highest temperatures reported by location and month.
# # A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.

# # Extra information:
# # The candidate can choose which kind of data store or data formats are used as a repository of data for steps 2 and 3.
# # The deliverable should contain a docker-compose file so it can be run by running ‘docker-compose up’ command. If the workflow relies on any database or any other middleware, this docker-compose file should have all what is necessary to make the workflow work (except passwords for the API or any other secret information)
# # The code should be well structured and add necessary log traces to easily detect problems. 

from pyspark.sql.functions import lit, udf, col, to_json, from_json, explode, avg, min, max, to_date, from_unixtime, date_format
from pyspark.sql.types import StructField, StructType, StringType
import json
import requests
from datetime import datetime, date, timedelta

spark.conf.set("spark.sql.sources.partitionOverwriteMode", 'dynamic' )

def request_api(lati_param, long_param, dt_timestamp_param, country_param, unit_param, apiKey):
  """ 
    Request Api - accessing Api accord the parameters as Lat, Long, date, contry, unit
    
    Parameters
    ----------
        lati_param : str
            Latitude value
        long_param : str
            Longitude value    
        dt_timestamp_param : timestamp
            timestamp based on the filter in the weather which day we are looking for
        country_param : str
            add country value into the return
        unit_param : str
              standard - default
              metrics - Celsius ( this is what i choose )
              imperial - Fahrenheit 
        apiKey : API KEY secret
              
    Returns
    -------
        return the parameter value as string
  """
  
  ## metric - Celsius values
  url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lati_param}&lon={long_param}&units={unit_param}&dt={dt_timestamp_param}&appid={apiKey}"
  local_request = requests.get(url)
  
  data_value = local_request.json()
  data_value["country"] = country_param
  return json.dumps(data_value)
 
  
def return_countries():
  """ 
  OBS:
  I have got those infos from https://www.distancelatlong.com/all/countries/ in order to get Lat and Long, 
  I noticed that we also can get from f"https://api.openweathermap.org/geo/1.0/direct?q=Brasília&limit=100&appid=xxxxxxxx 
  using Capital as key but in this case to keep simple i decided to get from website


  Description: Collect 10 Countries with Capital, Latitude and Longitude
  Returns
  -------
      return list countries info
  """

  list_countries = [
    { 'Country': 'Argentina',
     'Capital'  : 'Buenos Aires',
     'Latitude' :  '-51.65003986',
     'Longitude' : '-72.30001612' },
     {'Country': 'Brazil',
     'Capital'  : 'Brasília',
     'Latitude' :  '-5.809995505',
     'Longitude' : '-46.14998438' },
     {'Country': 'Colombia',
      'Capital'  : 'Bogotá',
      'Latitude' :  '5.346999095',
      'Longitude' : '-72.4059986' },
      {'Country': 'Croatia',
      'Capital'  : 'Zagreb',
      'Latitude' :  '43.7272222',
      'Longitude' : '15.9058333' },
      {'Country': 'Finland',
      'Capital'  : 'Helsinki',
      'Latitude' :  '60.99699611',
      'Longitude' : '24.47199954' },
      {'Country': 'France',
      'Capital'  : 'Paris',
      'Latitude' :  '45.89997479',
      'Longitude' : '6.116670287' },
      {'Country': 'Italy',
      'Capital'  : 'Rome',
      'Latitude' :  '40.64200213',
      'Longitude' : '15.7989965' },
      {'Country': 'Portugal',
      'Capital'  : 'Lisbon',
      'Latitude' :  '40.64100311',
      'Longitude' : '-8.650997534' },
      {'Country': 'India',
      'Capital'  : 'New Delhi',
      'Latitude' :  '15.491997',
      'Longitude' : '73.81800065' },
      {'Country': 'Israel',
      'Capital'  : 'Jerusalem',
      'Latitude' :  '31.91670012',
      'Longitude' : '34.86670252' 
      }
  ]
  
  return list_countries

#number_requests = 24 hours * 5 days * 10 countries = 1200

def main() -> None:
  """ 
    main code for step1
    Description: Save repo as raw and get infos from the api using lat, long
    Saved as parquet and i decided to partitioned by contry and date as overwrite process
    """
  
  try:
    logger = LoggingConsole(component='WeatherApi')  

    ## saving as last 5 days from today
    number_days_param = int(get_param('number_days_param'))
    apiKey = get_param('APIKEY')
    
    jsonDataList = []

    current_date = datetime.today().strftime('%Y-%m-%d')

    for date in generate_timestamp(date_param=current_date, number_days=number_days_param):
      dt_timestamp = date.strftime("%s")
      lst_countries = return_countries() 

      for icountry in lst_countries:
        logger.log_info(f"Generating in {date} for country {icountry['Country']}")

        request_value = request_api(icountry['Latitude'], icountry['Longitude'], dt_timestamp, icountry["Country"], 'metric', apiKey )
        jsonDataList.append(request_value)
        logger.log_info(f"Added the request return")
        

    jsonRDD = sc.parallelize(jsonDataList)
    logger.log_info(f"Reading Json")
    df = spark.read.json(jsonRDD)

    logger.log_info(f"Transformation - Flatten")
    df_explode = df.withColumn( 'data_struct', explode('data') )
    df_explode = df_explode.select(*(df.columns), 'data_struct.*' ).drop("data_struct").drop("data")
    df_explode = df_explode.withColumn("date", to_date(from_unixtime(col('dt'))) )
    df_explode = df_explode.withColumn("month", date_format(to_date("date", "dd-MM-yyyy"), "MM"))
    df_explode = df_explode.withColumn("year", date_format(to_date("date", "dd-MM-yyyy"), "yyyy"))

    raw_path = "/mnt/raw_weather_history/"
    table_name = "default.tb_raw_weather"
    logger.log_info(f"Save into the {raw_path} - table {table_name} ")

    (df_explode.write
     .mode("overwrite")
     .format('parquet')
     .partitionBy("country","date")
     .option('path', raw_path)
     .saveAsTable(table_name))
  except Exception as e:
    logger.log_info(f"Error {e}")
    
if __name__ == '__main__':
  main()


# COMMAND ----------

# MAGIC %md # Stage2 -  A dataset containing the location, date and temperature of the highest temperatures reported by location and month.

# COMMAND ----------

def main() -> None:
  logger = LoggingConsole(component='WeatherApi-Step2')  

  df_result_group = spark.sql(" select * from default.tb_raw_weather" )
  df_result_group = df_result_group.withColumnRenamed("country", "location")
  df_result_group = ( df_result_group
                            .groupBy("location","month")
                            .agg(max("temp").alias("highest_temp"))
                      )
  
  raw_path = "/mnt/weather_highest_temp/"
  table_name = "default.tb_weather_highest_temp"
  logger.log_info(f"Save into the {raw_path} - table {table_name} ")
  
  (df_result_group.write
   .mode("overwrite")
   .format('parquet')
   .partitionBy("location","month")
   .option('path', raw_path)
   .saveAsTable(table_name))

  spark.sql("select location, month, highest_temp from default.tb_weather_highest_temp").display()

if __name__ == '__main__':
    main() 

# COMMAND ----------

# MAGIC %md # Stage 3 - A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.

# COMMAND ----------

#to_date(from_unixtime($"ts" / 1000))
from pyspark.sql.functions import row_number, date_format, max
from pyspark.sql.Window import window

windowSpec  = Window.partitionBy("location").orderBy("temp")

def main() -> None:
  logger = LoggingConsole(component='WeatherApi-Step3')  
  
  df_result_group = spark.sql(" select * from default.tb_raw_weather" )
  df_result_group = df_result_group.withColumnRenamed("country", "location")
  
  df_result_group = (df_result_group
                          .groupBy("location","date")
                          .agg(avg("temp")
                          .alias("average_temp"),
                                  min("temp").alias("min_temp"),
                                  max("temp").alias("max_temp"))
                           )
  
  raw_path = "/mnt/weather_min_max_temp/"
  table_name = "default.tb_min_max_temp"
  logger.log_info(f"Save into the {raw_path} - table {table_name} ")
  
  (df_result_group.write
   .mode("overwrite")
   .format('parquet')
   .partitionBy("location","date")
   .option('path', raw_path)
   .saveAsTable(table_name))
  
  spark.sql("select location, date, average_temp, min_temp, max_temp from default.tb_min_max_temp").display()

if __name__ == '__main__':
    main() 

# COMMAND ----------

# MAGIC %md # test checking MIN and MAX per country by window func

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, min, max

df = spark.sql(" select country, date, dt,  month, temp, lat, lon from default.tb_raw_weather" )
windowSpecMax = Window.partitionBy("country").orderBy(col("temp").desc() )
windowSpecMin = Window.partitionBy("country").orderBy(col("temp").asc() )

df = (df.withColumn("row",
                    row_number().over(windowSpec))
        .withColumn("max_temp", max(col("temp")).over(windowSpecMax))
        .withColumn("min_temp", min(col("temp")).over(windowSpecMin))
        .filter(col("row") == 1)
        .select("country","min_temp","max_temp"))
    
df.display()


# COMMAND ----------

# MAGIC %md #  Validation and checking info for Argentina and date = '2023-04-08' -> max for this date is 9.21 and min 1.57 seems to be correct the Stage3

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, date, dt,  month, temp, lat, lon from default.tb_raw_weather where country = 'Argentina' and date = '2023-04-08'
