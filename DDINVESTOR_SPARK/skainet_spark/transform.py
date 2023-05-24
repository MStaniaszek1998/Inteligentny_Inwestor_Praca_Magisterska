

from pyspark.sql.types import StructField, StructType, StringType, BooleanType,DoubleType, \
ByteType, IntegerType, TimestampType, DateType, FloatType
import pyspark.sql.functions as F
from .logging import Extractor


class BaseTransformer:
    def __init__(self, path):
        self.path = path

    @property
    def path(self):
        return self._path
    
    @path.setter
    def path(self, value):
        base_path = './data'
        self._path = f'{base_path}{value}'


class Input(BaseTransformer):
    def __init__(self, path, spark=None):
        super().__init__(path)
        self.spark = spark

    def __call__(self):
        return self.spark.read.format("parquet").load(self.path)


class Output(BaseTransformer):
    def __init__(self, path, spark=None):
        super().__init__(path)
        
    def __call__(self):
        return self.path


class Metadata(BaseTransformer):
    meta_schema = StructType([
        StructField("field_name", StringType(), False),
        StructField("field_type", StringType(), False),
        StructField("nullable", BooleanType(), False)
    ])
    
    type_mapping = {
        'string': StringType(),
        'int8': ByteType(),
        'int16': IntegerType(),
        'float32': FloatType(),
        'timestamp': TimestampType(),
        'date': DateType(),
        'bool': BooleanType(),
        'double':DoubleType()
    }
    
    def __init__(self, path, spark=None):
        super().__init__(path)
        self.spark = spark
        
    def _read_metadata(self):
        df = (self.spark.read
              .format("csv")
              .option("header", "true")
              .schema(self.meta_schema)
              .load(self.path))
        return df
    
    def _map_type(self, field_type):
        return self.type_mapping[field_type]
    
    def _get_row(self, df):
        for row in df:
            yield row[0], self._map_type(row[1]), row[2]
            
    def _create_schema(self):
        df = self._read_metadata().collect()
        schema = StructType([StructField(a, b, c) for (a, b, c) in self._get_row(df)])
        return schema
        
    def __call__(self):
        return self._create_schema()


def transform(*t_args, **t_kwargs):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if 'params' in t_kwargs:
                params = t_kwargs['params']
                del t_kwargs['params']
            else:
                params = {}
            result = func(*t_args, **t_kwargs)
            # export paths to external dataset
            Extractor.save_transform_info(*t_args, t_kwargs, params)
            return result
        return wrapper
    return decorator

def print_statistics(pipeline):
    pipeline.dataframe.printSchema()
    pipeline.show_dimensions()

def assign_shortcuts(df,shortcut):

    df = df.select([F.col(c).alias(c+shortcut) for c in df.columns])

    return df
    
