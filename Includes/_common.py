# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems@e8183eed9481624f25b34436810cf6666b4438c0 \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest@96351a554ed00b664e7c2d97249805df5611be06 \
# MAGIC git+https://github.com/databricks-academy/dbacademy-helper@3a3a27b743283e24fe52f1f03ecc963d896f108e \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

import re
from dbacademy_gems import dbgems
from dbacademy_helper import DBAcademyHelper, Paths

helper_arguments = {
    "course_code" : "asp",             # The abreviated version of the course
    "course_name" : "apache-spark-programming-with-databricks",      # The full name of the course, hyphenated
    "data_source_name" : "apache-spark-programming-with-databricks", # Should be the same as the course
    "data_source_version" : "v03",     # New courses would start with 01
    "enable_streaming_support": True,  # This couse uses stream and thus needs checkpoint directories
    "install_min_time" : "2 min",      # The minimum amount of time to install the datasets (e.g. from Oregon)
    "install_max_time" : "5 min",      # The maximum amount of time to install the datasets (e.g. from India)
    "remote_files": remote_files,      # The enumerated list of files in the datasets
}

