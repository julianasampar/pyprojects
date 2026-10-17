# Script dedicated to profiling local datasets and writing results into a local JSON file

from claude.agents.data_discovery_agent.utils import reader
from claude.agents.data_discovery_agent.profiling import profiler
from claude.agents.data_discovery_agent.utils import writer

# How to use it - comment out the code below and replace <csv_path> and <output_path>

# source  = reader.get_datasource("csv",\
    #  folder_path="<csv_path>")
# results = profiler.profile_all_tables(source)
# storage = writer.get_storage('local', folder_path="<output_path>")
# storage.write_json_to_storage(content=results)