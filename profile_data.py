# Script dedicated to profiling local datasets and writing results into a local JSON file

from claude.agents.data_discovery_agent.utils import reader
from claude.agents.data_discovery_agent.profiling import profiler

# How to use it:
# source  = reader.get_datasource("csv",\
    #  folder_path="/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/airbnb/rio_de_janeiro")
#results = profiler.profile_all_tables(source)