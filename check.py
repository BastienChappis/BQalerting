from tools import Directory, Table, Query

class TableCheck:
    def __init__(self, client, table, debug=False):
        self.table_to_check=table
        self.client=client
        self.sql_snippet=""
        self.debug=debug
        self.metadataframe=self._check_table_metadata()
        self.checklist=[]

        
# ------------------------------   
# Initial checks
#-------------------------------        
        
    def _check_table_metadata(self):
        print("> Retrieve table metadata")
        sql_snippet=f"""
        SELECT 
            column_name,
            data_type,
            is_partitioning_column,
            clustering_ordinal_position
        FROM `{self.table_to_check.project}`.{self.table_to_check.dataset}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{self.table_to_check.table}'

        """
        temp_query=Query(self.client, sql_snippet)
        if self.debug:
            temp_query.display()
        self.metadataframe=temp_query.to_df_light()
        return(self.metadataframe)
        
        
    def _check_column_exists(self, column):
        if self.debug:
            print(f'> checking column {column} is existing')
        assert column in list(self.metadataframe['column_name']), f'> {column} is not present in the table' 
        
    def _unicode_clean(self,unicode):
        return(unicode.translate({ord(c): None for c in '-&é()çà$*%ùè`~!@#$'}))
    
# ------------------------------   
# Column check
#-------------------------------

    def expect_column_value_to_not_be_null(self, column):
        self._check_column_exists(column)
        
        check_name=f'expect_{column}_value_to_not_be_null'
        sql_snippet=f"IF(COUNTIF({column} IS NULL) = 0, True, False) AS {check_name}"
        sql_snippet+=",\n"
        self.sql_snippet+=sql_snippet
        self.checklist.append(check_name)
        return(self)
    
    def expect_column_value_mean_to_be_between(self, column, value1, value2):
        self._check_column_exists(column)
        assert type(value1)==int or type(value1)==float, "value1 must be integer or float"
        assert type(value2)==int or type(value2)==float, "value2 must be integer or float"
        
        check_name=f'expect_{column}_value_mean_to_be_between'
        sql_snippet=f"IF(AVG({column})BETWEEN {value1} AND {value2}, True, False) AS {check_name}"
        sql_snippet+=",\n"

        self.sql_snippet+=sql_snippet
        self.checklist.append(check_name)
        return(self)
    
    def expect_column_values_to_be_unique(self, column):
        self._check_column_exists(column)
        check_name=f'expect_{column}_column_values_to_be_unique'
        sql_snippet=f"IF(COUNT({column}) = COUNT(DISTINCT {column}), True, False) AS {check_name}"
        sql_snippet+=",\n"
        self.sql_snippet+=sql_snippet
        self.checklist.append(check_name)
        return(self)

    def expect_column_values_to_match_regex(self, column, regex):
        self._check_column_exists(column)
        check_name=f'expect_column_{column}_values_to_match_regex_{self._unicode_clean(regex)}'
        sql_snippet=f"IF(MIN(IF(REGEXP_CONTAINS(CAST({column} AS STRING), r'{regex}'), 1 ,0))=1, True, False) AS {check_name}"
        sql_snippet+=",\n"
        self.sql_snippet+=sql_snippet
        self.checklist.append(check_name)
        return(self)
    
    def expect_column_values_to_not_match_regex(self, column, regex):
        self._check_column_exists(column)
        check_name=f'expect_column_{column}_values_to_not_match_regex_{self._unicode_clean(regex)}'
        sql_snippet=f"IF(MAX(IF(REGEXP_CONTAINS(CAST({column} AS STRING), r'{regex}'), 1, 0))=0, True, False) AS {check_name}"
        sql_snippet+=",\n"
        self.sql_snippet+=sql_snippet
        self.checklist.append(check_name)
        return(self)

# ------------------------------   
# Row check
#-------------------------------

    def expect_table_row_count_to_be_between(self, column, value1, value2):
        assert type(value1)==int or type(value1)==float, "value1 must be integer or float"
        assert type(value2)==int or type(value2)==float, "value2 must be integer or float"
        
        check_name=f'expect_table_row_count_to_be_between'
        sql_snippet=f"IF(COUNT(*)BETWEEN {value1} AND {value2}, True, False) AS {check_name}"
        sql_snippet+=",\n"
        self.sql_snippet+=sql_snippet
        self.checklist.append(check_name)
        return(self)
    

        
    def run(self):
        for check in self.checklist:
            print(f"> Checking {check}")
        sql_snippet=f"""
        SELECT
            check,
            result,
            CURRENT_DATE() AS check_date,
            IF(result = TRUE, "🟢", "🔴") AS pass_fail
        FROM(
            SELECT
                {self.sql_snippet}
            FROM {self.table_to_check.path("standard")}
        )
        UNPIVOT(result FOR check IN ({','.join([i for i in self.checklist])}))

        
        """
        temp_query=Query(self.client, sql_snippet)
        if self.debug:
            temp_query.display()
        elif not self.debug:
            return(temp_query.to_df_light())
