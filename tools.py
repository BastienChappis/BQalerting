#-------------------------------
#        libraries
#-------------------------------

#bigquery sdk
from google.cloud import bigquery

#formating related libraries
from pygments import highlight, lexers, formatters
from pygments_pprint_sql import SqlFilter
from colorama import Fore, Back, Style

from datetime import datetime
from time import sleep

#-------------------------------
#        diretory
#-------------------------------

class Directory:
    '''
    Creates BigQuery directory

            Parameters:
                    project (str): Name of the project
                    dataset (str): Name of the dataset
    '''
    def __init__(self, client, project, dataset):
        self.project = project
        self.dataset = dataset
        self.client = client
    
    def check_if_exist(self):
        try:
            client.get_dataset(self.dataset)  # Make an API request.
            print("Dataset {} already exists".format(self.dataset))
            return(True)
        except NotFound:
            print("Dataset {} is not found".format(self.dataset))
            return(False)
    
    def create(self, location):
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(self.dataset)

        # TODO(developer): Specify the geographic location where the dataset should reside.
        dataset.location = location
        assert type(location)==str, 'location must be a string'

        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))
        
        
class Table():
    '''
    Creates BigQuery table path from a directory

            Parameters:
                    directory (object): directory
                    table (str): Name of the table
    '''
    
    def __init__(self, client, directory, table):
        self.project = directory.project
        self.dataset = directory.dataset
        self.table = table
        self.client = client
        
    def path(self, language, display=False, table_suffix=""):
        '''
        Returns the full path of a BigQuert directory object 
        
                Parameters:
                        language (str): legacy or standard
                        display* (boolean): prints the directory once the function is completez
                        table_suffix* (str): string to be added at the end of the table path, separator is already handled by the function
                        
        '''
        #checks
        if len(table_suffix)> 0 :#if there is a table suffix, adds a separator
            if table_suffix == "_":
                table_suffix = "_"
            else:
                table_suffix = "_" + str(table_suffix)
        if language == "standard":
            self.dir = "`{project}.{dataset}.{table}{suffix}`".format(project = self.project, dataset = self.dataset, table = self.table, suffix = table_suffix)              
        elif language == "legacy":
            self.dir = "{project}:{dataset}.{table}{suffix}".format(project = self.project, dataset = self.dataset, table = self.table, suffix = table_suffix)
        if display:
            print("> directory = {}".format((self.dir)))
        return(self.dir)
    
    def check_if_exist(self):
        
        try:
            self.client.get_table(self.table)  # Make an API request.
            print("Table {} already exists.".format(self.table))
            return(True)
        except NotFound:
            print("Table {} is not found.".format(self.table))
            return(False)
    

#-------------------------------
#          Query
#-------------------------------   

class Query:
    
    def __init__(self, client, query):
        self.client = client
        self.query = query
        
    def _check_query_job_state(self, query_job):
        # Check on the progress by getting the job's updated state. Once the state
        # is `DONE`, the results are ready.
        query_job = self.client.get_job(
            query_job.job_id, location=query_job.location
        )  # Make an API request.
        print(f"> Job {query_job.job_id} is currently {query_job.state}", end='')
        while query_job.state=='RUNNING ':
            print(">", end='')
            query_job = self.client.get_job(
                query_job.job_id, location=query_job.location
                )  
            sleep(0.5) 
        print(Fore.GREEN + f"\n> Query {query_job.state} (ಠ‿↼)") 
        print(Style.RESET_ALL)
        
    def _retrieve_query_job_metadata(self, query_job):
        try:
            print(Fore.MAGENTA + f"> Email: {query_job.user_email}")
            print(f"> Job time: {query_job.created}")
            print(f"> Billed MBytes: {query_job.total_bytes_billed/1e6}")
            print(f"> Total query time: {query_job.slot_millis}ms")
            print(Style.RESET_ALL)
        except:
            print(Fore.RED + f"> Error printing metadata")
            print(Style.RESET_ALL)

    
    def display(self):
        '''
        Returns a beautyfied and readable version of the query
                        
        '''
        lexer = lexers.MySqlLexer()
        return(print(highlight(self.query, lexer, formatters.TerminalFormatter())))
            
                
    def execute(self, dry_run=False):
        '''
        Executes the query
        
                Parameters:
                        dry_run: (bool) if set to True, runs an estimation of the costs
                        
        '''        
        # Set up query job configs
        job_config = bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=False)
        
        query_job=self.client.query(
            self.query,
            job_config=job_config
        )
        
        if dry_run:
            # A dry run query completes immediately.
            print(f"> This query will process {query_job.total_bytes_processed} bytes.")
        
        self._check_query_job_state(query_job)
        
        
    
    def to_table(self, endpoint, table_suffix="", write_disposition='WRITE_TRUNCATE', sequence=True):
        '''
        Creates a new table from the query
        
                Parameters:
                        endpoint: (obj:Table) output table path
                        table_suffix: (str) any variable suffix to add at the end of the table ("_" will be added automatically)
                        write_disposition: (str)
                        sequence: (bool) if set to True, wait for the execution of the job            
        '''
        
        if not sequence:
            print(Fore.RED + f"\n> Sequencing is deactivated, job status wont be verified" + Style.RESET_ALL) 
        #if there is a table suffix, a sperator must be added
        if len(table_suffix) > 0 and '$' not in table_suffix :
            table_suffix = "_" + str(table_suffix)
        # Prepare a reference to a new dataset for storing the query results.   
        job_config = bigquery.QueryJobConfig()
        # Set the destination table to where you want to store query results.
        # As of google-cloud-bigquery 1.11.0, a fully qualified table ID can be
        # used in place of a TableReference.
        job_config.destination = f"{endpoint.project}.{endpoint.dataset}.{endpoint.table}{table_suffix}"
        job_config.write_disposition = write_disposition
        query = self.query
        # Run the query.
        print(f'> Exporting query results to table {job_config.destination}')
        query_job = self.client.query(query, job_config=job_config)
        
        if sequence:
            query_job = self.client.get_job(
                query_job.job_id, location=query_job.location
            )  # Make an API request.

            self._check_query_job_state(query_job)
            self._retrieve_query_job_metadata(query_job)

            
    def to_df_light(self):
        '''
        Transfers the results of the query to a dataframe
        Recommended only when the output result is light
        
                Parameters:
                        sequence: (bool) if set to True, wait for the execution of the job            
        ''' 

        # Prepare a reference to a new dataset for storing the query results.   
        job_config = bigquery.QueryJobConfig()
        # Set the destination table to where you want to store query results.
        # As of google-cloud-bigquery 1.11.0, a fully qualified table ID can be
        # used in place of a TableReference.
        query = self.query
        # Run the query.
        query_job = self.client.query(query, job_config=job_config)

        #query_job.result()  # Waits for the query to finish
        
        query_job = self.client.get_job(
            query_job.job_id, location=query_job.location
        )  # Make an API request.
        

        self._check_query_job_state(query_job)
        self._retrieve_query_job_metadata(query_job)
        return(query_job.result().to_dataframe())

#-------------------------------
#          Dataframe
#-------------------------------   

class Dataframe:
    def __init__(self, client, dataframe):
        self.client = client
        self.dataframe = dataframe
        
    def _check_job_state(self, query_job):
        # Check on the progress by getting the job's updated state. Once the state
        # is `DONE`, the results are ready.
        query_job = self.client.get_job(
            query_job.job_id, location=query_job.location
        )  # Make an API request.
        print(f"> Job {query_job.job_id} is currently {query_job.state}", end='')
        while query_job.state=='RUNNING':
            print("=", end='')
            query_job = self.client.get_job(
                query_job.job_id, location=query_job.location
                )  
            sleep(0.5) 
        print(Fore.GREEN + f"\n> Query {query_job.state} (ಠ‿↼)") 
        print(Style.RESET_ALL)
        
    def _retrieve_job_metadata(self, query_job):
        print(Fore.MAGENTA + f"> Email: {query_job.user_email}")
        print(f"> Job time: {query_job.created}")
    
    def to_table(self, endpoint, table_suffix="", write_disposition='WRITE_TRUNCATE', sequence=True):
        if not sequence:
            print(Fore.RED + f"\n> Sequencing is deactivated, job status wont be verified" + Style.RESET_ALL) 
        #if there is a table suffix, a sperator must be added
        if len(table_suffix) > 0 and '$' not in table_suffix :
            table_suffix = "_" + str(table_suffix)
            
        table_id = f'{endpoint.project}.{endpoint.dataset}.{endpoint.table}{table_suffix}'
        
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
    #         schema=[
    #             # Specify the type of columns whose type cannot be auto-detected. For
    #             # example the "title" column uses pandas dtype "object", so its
    #             # data type is ambiguous.
    #             bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
    #             # Indexes are written if included in the schema by name.
    #             bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
    #         ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_TRUNCATE",
        )
        print(f'> Exporting dataframe to table {table_id}')
        job = self.client.load_table_from_dataframe(
            self.dataframe, table_id, job_config=job_config
        )  # Make an API request.
        if sequence:
            
            job = self.client.get_job(
                job.job_id, location=job.location
            )  # Make an API request.
        
            self._check_job_state(job)
            self._retrieve_job_metadata(job)        
            table = self.client.get_table(table_id)  # Make an API request.

            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )
            return(job)

#########################################  Bucket class  ###################################################
class Bucket:
    def __init__(self, gcs_client, bucket_name):
        self.bucket_name = bucket_name
        self.gcs_client = gcs_client
    
    def check_if_exists(self):
        try:
            metadata=self.gcs_client.get_bucket_metadata(self.bucket_nam)
            exists=1
            print("> bucket found in location = {}".format(metadata["location"]))
        except:
            pass

        return(print("\U0001F7E2") if exists else print("\U0001F534"))
#########################################  date function  ###################################################                

def date_range(start_date, end_date):
    '''
        Creates a lsit of date from the input start and end date in the string format
        
                Parameters:
                        start_date: start date in the format "YYYYmmdd"
                        end_date: end date in the format "YYYYmmdd"
            
    '''  
    
    try:
        print(Fore.YELLOW + "> List of date generated from {} to {}".format(start_date, end_date))
        start_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
        date_list = pd.date_range(start_date,end_date,freq='d')
        date_list_str = [datetime.strftime(day, "%Y%m%d") for day in date_list]
        return(date_list_str)

    except:
        print (Fore.RED + "> Date format raised an issue")

