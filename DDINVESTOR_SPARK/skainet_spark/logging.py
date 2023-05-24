import os
import time

import pandas as pd

class Extractor:
    @staticmethod
    def save_transform_info(spark, kwargs, params):
        def get_last_modified_date(path):
            last_modified = os.path.getmtime(path)
            modificationTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_modified))
            return modificationTime

        def parse_path(path):
            namespace = os.path.dirname(path)
            file = os.path.basename(path).split('.')
            filename = file[0]
            if len(file)>1:
                extension = file[1]
            else:
                extension = None
            return namespace, filename, extension

        def parse_namespace(namespace):
            return namespace.split('/')

        def enable_replace(path):
            if os.path.exists(path):
                return False
            return True

        def to_csv(dataframe, path):
            if enable_replace(path):
                dataframe.to_csv(path, index=False)
            else:
                dataframe.to_csv(path, mode='a', index=False, header=False)

        def save_data(data, path):
            dataframe = pd.DataFrame()
            to_csv(dataframe.append(data, ignore_index=True), path)
        
        def extract(spark, transform_name, transform_type, path):
            namespace, filename, extension = parse_path(path)
            namespace_parts = parse_namespace(namespace)

            data = {
                'path': path,
                'app_name': spark.sparkContext.appName,
                'app_id': spark.sparkContext.applicationId,
                'transform_name': transform_name,
                'transform_type': transform_type,
                'stage': namespace_parts[-1],
                'project': namespace_parts[-2],
                'filename': filename,
                'extension': extension,
                'modified': get_last_modified_date(path)
            }
            base_path = './data'
            data_path = f'{base_path}/_file_transforms/file_transforms.csv'
            save_data(data, data_path)
        
        def find_output_transform(spark, kwargs):
            for key in kwargs:
                if type(kwargs[key]).__name__ == 'Output':
                    namespace, _, _ = parse_path(kwargs[key].path)
                    stage = parse_namespace(namespace)[-1]
                    if stage != 'exception':
                        return f'{spark.sparkContext.appName}_{stage}'
            return None
            

        def get_default_transform_name(spark, kwargs):
            return find_output_transform(spark, kwargs)

        if 'transform_name' in params:
            transform_name = params['transform_name']
        else:
            transform_name = get_default_transform_name(spark, kwargs)

        for key in kwargs:
            if '_extracting_disabled' in key:
                continue
            extract(
                spark,
                transform_name=transform_name,
                transform_type=type(kwargs[key]).__name__,
                path=kwargs[key].path
                   )
