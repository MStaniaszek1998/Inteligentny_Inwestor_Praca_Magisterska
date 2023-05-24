from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F

from .my_exceptions import MyException
from .pipeline import Pipeline
from .transform import Input, Output, Metadata

class ValidatedPipeline:
    def __init__(self, dataframe, schema):
            
        self._conditions = []
        
        if isinstance(schema, Metadata):
            self.schema = schema()
        elif isinstance(schema, StructType):
            self.schema = schema
        else:
            raise Exception(MyException.UNRECOGNIZED_SCHEMA_FORMAT)
        
        if isinstance(dataframe, Input):
            self.dataframe = dataframe()
        elif isinstance(dataframe, Pipeline):
            self.dataframe = dataframe.dataframe
        elif isinstance(dataframe, DataFrame):
            self.dataframe = dataframe
        else:
            raise Exception(MyException.UNRECOGNIZED_DATAFRAME_FORMAT)
    
    @property
    def schema(self):
        return self._schema
    
    @schema.setter
    def schema(self, schema):
        for key in schema.fieldNames():
            if schema[key].nullable == False:
                self._conditions.append((F.col(key).isNotNull(), f'Column "{key}" not nullable'))
        self._schema = schema
    
    @property
    def dataframe(self):
        return self._dataframe
    
    @dataframe.setter
    def dataframe(self, dataframe):
        self._dataframe = self._validate_schema(dataframe, self.schema)
        
    def _validate_schema(self, dataframe, schema):
        
        def validate_column_names(dataframe, schema):
            if set(dataframe.columns) != set(schema.fieldNames()):
                missing_columns = set(schema.fieldNames()) - set(dataframe.columns)
                unexpected_columns = set(dataframe.columns) - set(schema.fieldNames())
                msg = f'\nMissing columns: {missing_columns} \nUnexpected columns: {unexpected_columns}'
                raise Exception(MyException.SCHEMA_COLUMNS_NOT_MATCHED + msg)
        
        def validate_datatypes(defacto_schema, dejure_schema):
            mismatched_types = {}
            validated = True
            for field in defacto_schema.fieldNames():
                if defacto_schema[field].dataType != dejure_schema[field].dataType:
                    validated = False
                    mismatched_types[field] = (defacto_schema[field].dataType, dejure_schema[field].dataType)
            if not validated:
                msg = f'\nMismatched types (de facto, de jure):\n{mismatched_types}'
                raise Exception(MyException.SCHEMA_DATATYPES_NOT_MATCHED + msg)
        
        validate_column_names(dataframe, schema)
        validate_datatypes(dataframe.schema, schema)
   
        return dataframe

    def add_validation(self, condition, exception_msg):
        self._conditions.append((condition, exception_msg))
        return self

    def validate(self):
        self._dataframe = self._dataframe.withColumn('validated', F.lit(True))
        self._dataframe = self._dataframe.withColumn('exception_msg', F.lit(None))
        for condition, exception_msg in self._conditions:
            self._dataframe = self._dataframe.withColumn('exception_msg', 
                                                         F.when(condition,
                                                                F.col('exception_msg'))
                                                          .otherwise(F.lit(exception_msg)))
            self._dataframe = self._dataframe.withColumn('validated', 
                                                         F.when(condition, 
                                                                F.col('validated'))
                                                          .otherwise(F.lit(False)))
        
        self._dataframe.cache()
        self.exception_dataframe = self._dataframe.where(~F.col('validated'))
        self.exception_dataframe = self.exception_dataframe.drop(*['validated'])

        self.validated_dataframe = self._dataframe.where(F.col('validated'))
        self.validated_dataframe = self.validated_dataframe.drop(*['validated', 'exception_msg'])

        self.print_counts()
            
        return self

    def print_counts(self):
        validated_count = self.validated_dataframe.count()
        exception_count = self.exception_dataframe.count()
        print('Validated count:', validated_count)
        print('Exception count:', exception_count)

    def write(self, output_path, exception_path=None):
        if isinstance(output_path, Output):
            output_path = output_path()
        self._write_validated_dataframe(output_path)
        
        if exception_path is not None:
            if isinstance(exception_path, Output):
                exception_path = exception_path()
            self._write_exception_dataframe(exception_path)

    def write_csv(self, output_path, exception_path=None):
        if isinstance(output_path, Output):
            output_path = output_path()
        self._write_validated_dataframe_csv(output_path)

        if exception_path is not None:
            if isinstance(exception_path, Output):
                exception_path = exception_path()
            self._write_exception_dataframe_csv(exception_path)
        
    def _write_validated_dataframe(self, path):
        self.validated_dataframe.write.format('parquet').mode('overwrite').save(path)
        
    def _write_exception_dataframe(self, path):
        self.exception_dataframe.write.format('parquet').mode('overwrite').save(path)

    def _write_validated_dataframe_csv(self, path):
        (self.validated_dataframe
         .repartition(1)
         .write.format('csv')
         .option("header", "true")
         .option('sep',',')
         .mode('overwrite')
         .save(path))

    def _write_exception_dataframe_csv(self, path):
        (self.exception_dataframe
         .repartition(1)
         .write.format('csv')
         .option('sep',',')
         .option("header", "true")
         .mode('overwrite')
         .save(path))
