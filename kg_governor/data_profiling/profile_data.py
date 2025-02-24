from datetime import datetime
import glob
import os
from pathlib import Path
import shutil
from typing import Tuple, List
import warnings
import hashlib

warnings.simplefilter('ignore')
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # set tensorflow log level to FATAL

import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import fasttext
import spacy

from kg_governor.data_profiling.fine_grained_type_detector import FineGrainedColumnTypeDetector
from kg_governor.data_profiling.profile_creators.profile_creator import ProfileCreator
from kg_governor.data_profiling.model.table import Table
from kg_governor.data_profiling.utils import generate_column_id
from kglids_config import KGLiDSConfig


def profile_data():
    start_time = datetime.now()
    print(datetime.now(), ': Initializing Spark')

    # initialize spark
    if KGLiDSConfig.is_spark_local_mode:
        spark = SparkContext(conf=SparkConf().setMaster(f'local[{KGLiDSConfig.spark_n_workers}]')
                             .set('spark.driver.memory', f'{KGLiDSConfig.spark_max_memory // 2}g'))
    else:
        spark = SparkSession.builder.appName("KGLiDSProfiler").getOrCreate().sparkContext
        # add python dependencies
        for pyfile in glob.glob(os.path.join(KGLiDSConfig.base_dir, 'kg_governor', 'data_profiling', '**', '*.py'),
                                recursive=True):
            spark.addPyFile(pyfile)
        # add embedding model files
        for embedding_file in glob.glob(os.path.join(KGLiDSConfig.base_dir, 'kg_governor', 'data_profiling',
                                                     'column_embeddings', 'pretrained_models', '**', '*.pt'),
                                        recursive=True):
            spark.addFile(embedding_file)
        # add fasttext embeddings file
        spark.addFile(os.path.join(KGLiDSConfig.base_dir, 'storage', 'embeddings', 'cc.en.50.bin'))

    # get the list of columns and their associated tables
    print(datetime.now(), ': Creating tables, Getting columns')
    columns_and_tables = []

    for dataset in os.listdir(KGLiDSConfig.data_source_path):
        for filename in glob.glob(os.path.join(KGLiDSConfig.data_source_path, dataset, '**', '*.csv'), recursive=True):
            if os.path.isfile(filename) and os.path.getsize(filename) > 0:  # if not an empty file
                table = Table(data_source=KGLiDSConfig.data_source,
                              table_path=filename,
                              dataset_name=dataset)
                # read only the header
                try:
                    header = pd.read_csv(table.get_table_path(), nrows=0, engine='python', encoding_errors='replace')
                except:
                    continue
                columns_and_tables.extend([(col, table) for col in header.columns])

    # delete existing profiles if necessary
    if os.path.exists(KGLiDSConfig.profiles_out_path):
        if KGLiDSConfig.replace_existing_profiles:
            print(datetime.now(), ': Deleting existing column profiles in:', KGLiDSConfig.profiles_out_path)
            shutil.rmtree(KGLiDSConfig.profiles_out_path)
        else:
            # skip existing profiles
            existing_profiles = set(
                [Path(p).stem for p in glob.glob(os.path.join(KGLiDSConfig.profiles_out_path, '**', '*.json'),
                                                 recursive=True)])
            print(datetime.now(), f': Skipping {len(existing_profiles)} existing profiles.')
            for i in range(len(columns_and_tables)):
                column_id = generate_column_id(columns_and_tables[i][1].get_data_source(),
                                               columns_and_tables[i][1].get_dataset_name(),
                                               columns_and_tables[i][1].get_table_name(),
                                               str(columns_and_tables[i][0]))
                column_profile_name = hashlib.md5(column_id.encode()).hexdigest()
                if column_profile_name in existing_profiles:
                    columns_and_tables[i] = None
            columns_and_tables = [i for i in columns_and_tables if i]

    os.makedirs(KGLiDSConfig.profiles_out_path, exist_ok=True)

    # profile the columns with Spark.
    columns_and_tables_rdd = spark.parallelize(columns_and_tables, len(columns_and_tables) // 10)
    print(datetime.now(), f': Profiling {len(columns_and_tables)} columns')
    columns_and_tables_rdd.mapPartitions(
        lambda x: column_worker(column_names_and_tables=x, profiles_out_path=KGLiDSConfig.profiles_out_path)).collect()

    print(datetime.now(), f': {len(columns_and_tables)} columns profiled and saved to {KGLiDSConfig.profiles_out_path}')
    print(datetime.now(), ': Total time to profile: ', datetime.now() - start_time)


def column_worker(column_names_and_tables: List[Tuple[str, Table]], profiles_out_path: str):
    fasttext_model = fasttext.load_model(os.path.join(KGLiDSConfig.base_dir, 'storage/embeddings/cc.en.50.bin'))
    try:
        ner_model = spacy.load('en_core_web_sm')
    except:
        import subprocess

        subprocess.call('python -m spacy download en_core_web_sm'.split(), shell=False)
        ner_model = spacy.load('en_core_web_sm')
    for (column_name, table) in column_names_and_tables:
        # read the column from the table file. Use the Python engine if there are issues reading the file
        try:
            try:
                column = pd.read_csv(table.get_table_path(), usecols=[column_name], squeeze=True,
                                     na_values=[' ', '?', '-'])
            except:
                column = pd.read_csv(table.get_table_path(), usecols=[column_name], squeeze=True,
                                     na_values=[' ', '?', '-'],
                                     engine='python', encoding_errors='replace')

            column = pd.to_numeric(column, errors='ignore')
            column = column.convert_dtypes()
            column = column.astype(str) if column.dtype == object else column

            # infer the column data type
            column_type = FineGrainedColumnTypeDetector.detect_column_data_type(column, fasttext_model, ner_model)

            # collect statistics, generate embeddings, and create the column profiles
            column_profile_creator = ProfileCreator.get_profile_creator(column, column_type, table, fasttext_model)
            column_profile = column_profile_creator.create_profile()

            # store the profile
            column_profile.save_profile(profiles_out_path)
        except Exception as e:
            print(f'Warning: Skipping non-parse-able column: {column_name} in table: {table.get_table_path()}')
            print(e)
            continue
    return []
