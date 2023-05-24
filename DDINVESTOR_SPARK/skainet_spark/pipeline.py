from pyspark.sql.types import StringType, ByteType, IntegerType, TimestampType, BooleanType
import pyspark.sql.functions as F
from .transform import Input, Output

class Pipeline:
    def __init__(self, dataframe):
        if isinstance(dataframe, Input):
            self.dataframe = dataframe()
        else:
            self.dataframe = dataframe
        
    @property
    def dataframe(self):
        return self._dataframe
    
    @dataframe.setter
    def dataframe(self, value):
        self._dataframe = value
    
    def rename_columns(self, column_pairs: dict):
        for key in column_pairs:
            self._dataframe = self._dataframe.withColumnRenamed(key, column_pairs[key])
        return self

    def select(self, column_names: list):
        self._dataframe = self._dataframe.select(column_names)
        return self

    def distinct(self):
        self._dataframe = self._dataframe.distinct()
        return self

    def where(self, condition):
        self._dataframe = self._dataframe.where(condition)
        return self
    
    def add_columns(self, columns: dict):
        for key in columns:
            self._dataframe = self._dataframe.withColumn(key, columns[key])
        return self
    
    def drop_columns(self, column_names: list):
        self._dataframe = self._dataframe.drop(*column_names)
        return self
    
    def cast_columns(self, columns: dict):
        types = {
            'bool': BooleanType(),
            'int8': ByteType(),
            'int16': IntegerType(),
            'string': StringType(),
            'datetime': 'datetime'
        }
        for col in columns:
            spark_type = types[columns[col]]
            if spark_type == 'datetime':
                self._dataframe = self._dataframe.withColumn(col, F.to_timestamp(col, "yyyy-MM-dd HH:mm"))
            else:
                self._dataframe = self._dataframe.withColumn(col, F.col(col).cast(spark_type))
        return self
    
    def transform(self, func, *args, **kwargs):
        self._dataframe = func(self._dataframe, *args, **kwargs)
        return self

    def write_csv(self, path):
        if isinstance(path, Output):
            path = path()
        (self._dataframe
         .repartition(1)
         .write.format('csv')
         .option("header", "true")
         .mode('overwrite')
         .save(path))
    
    def write(self, path):
        if isinstance(path, Output):
            path = path()
        self._dataframe.write.format('parquet').mode('overwrite').save(path)
        
    def show_dimensions(self):
        cols = len(self._dataframe.columns)
        rows = self._dataframe.count()
        print('cols:', cols, 'rows:', rows)
        return self

    def union(self, union_with):
        if isinstance(union_with, Input):
            union_with = union_with()
        elif isinstance(union_with, Pipeline):
            union_with = union_with.dataframe
        self._dataframe = self._dataframe.union(union_with)
        return self

    def join(self, join_with, join_on, join_type):
        if isinstance(join_with, Input):
            join_with = join_with()
        elif isinstance(join_with, Pipeline):
            join_with = join_with.dataframe
        self._dataframe = self._dataframe.join(join_with, join_on, join_type)
        return self
