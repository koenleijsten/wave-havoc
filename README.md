# wave-havoc

This tool analyzes weather data to identify heatwaves based on specific criteria such as duration, number of tropical days, and maximum temperature.

## Setup

Before running the tool, make sure you have Python and Apache Spark installed on your system. You can install Python dependencies using poetry or pip:

```bash
pip install -r requirements.txt
```

## Usage

To use the tool, simply move to the src/wve_havoc directory and execute the `wave_havoc.py` file. The tool will load environment variables, download and extract weather data, perform analysis, and output the results in JSON format.

```bash
cd ./src/wave_havoc
python wave_havoc.py
```

## Configuration
The tool requires several environment variables to be set:

- NUM_WORKERS: Number of Spark workers to use for parallel processing.
- LOCATION: Location for which weather data will be analyzed.
- THRESHOLD_TEMP: Threshold temperature to identify heatwaves.
- TROPICAL_DAY_TEMP: Temperature threshold for a day to be considered tropical.
- DURATION_IN_DAYS_THRESHOLD: Minimum duration of a heatwave in days.
- NUMBER_OF_TROPICAL_DAYS_THRESHOLD: Minimum number of tropical days within a heatwave.

These variables can be set in a .env file in the project directory.

## Output
The tool outputs the identified heatwaves in JSON format. Each heatwave is represented by a JSON object containing the start date, end date, duration in days, number of tropical days, and maximum temperature.

## License
This project is licensed under the Apache License - see the LICENSE file for details.