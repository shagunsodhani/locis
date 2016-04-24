def parse(file_path):
    # Method to read the config file.
    # Using a custom function for parsing so that we have only one config for 
    # both the scripts and the mapreduce tasks.
    config = {}
    with open(file_path) as f:
        for line in f:
            data = line.strip()
            if(data and not data.startswith("#")):
                (key, value) = data.split("=")
                config[key] = value
    return config