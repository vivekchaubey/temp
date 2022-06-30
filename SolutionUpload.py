import os
import pandas as pd
from collections import Counter


class SolutionUpload:
    """Manulife take home assignment solution."""

    def __init__(self, input_path, output_file_name):
        """Constructor initialization and constants assigned here"""
        self.output_file_name = output_file_name
        self.path = input_path
        self.file_extension = '.csv'
        self.input_dataframes = None
        self.output_dataframe = None

    def extract(self):
        """Look for all the csv files in the path except for outfile if it exists.

           Returns a dictionary where keys are the input csv file names and values are
           the dataframe content of those file names
        """
        csv_file_names = [each for each in os.listdir(self.path) if each[-4:] == self.file_extension]
        input_csv_file_names = [each for each in csv_file_names if each != self.output_file_name]
        self.input_dataframes = {each: pd.read_csv(each) for each in input_csv_file_names}

    def empty_check(self):
        """Checking if the input folder is empty."""

        if len(list(self.input_dataframes.keys())) == 0:
            raise AssertionError("Empty Input Folder")

    def schema_match_check(self):
        """Check if the schema of the input files are matching"""

        schemas = {}
        for file_name in self.input_dataframes.keys():
            curr = tuple(sorted(list(self.input_dataframes[file_name].columns)))
            if curr not in list(schemas.keys()):
                schemas[curr] = [file_name]
            else:
                schemas[curr].append(file_name)
        if len(list(schemas.keys())) > 1:
            print(schemas)
            raise AssertionError("Schema does not match. Details as above")

    def source_ip_check(self):
        """Checking if Source IP column is in file or not"""

        for file_name in self.input_dataframes:
            if "Source IP" not in list(self.input_dataframes[file_name].columns):
                text = "Source IP column not in "+file_name
                raise AssertionError(text)

    def ip_check(self):
        def valid_ip_format(input_ip, file_name):
            bool_ip = True
            text = input_ip.split('.')
            if len(text) != 4:
                bool_ip = False
                raise AssertionError('Invalid IP in the file '+file_name)
            if 0 < int(text[0]) < 256:
                bool_ip = True
            else:
                bool_ip = False
                raise AssertionError('Invalid IP in the file '+file_name)
            if 0 <= int(text[1]) < 256:
                bool_ip = True
            else:
                bool_ip = False
                raise AssertionError('Invalid IP in the file '+file_name)
            if 0 <= int(text[2]) < 256:
                bool_ip = True
            else:
                bool_ip = False
                raise AssertionError('Invalid IP in the file '+file_name)
            if 0 <= int(text[3]) < 256:
                bool_ip = True
            else:
                bool_ip = False
                raise AssertionError('Invalid IP in the file'+file_name)

        for each_file in self.input_dataframes.keys():
            self.input_dataframes[each_file]['Source IP'].apply(lambda x: valid_ip_format(x, each_file))

    def transform(self):
        """Takes the extract function output as input
        and transform the data based on the given logic"""

        # creating an empty output dataframe
        self.output_dataframe = pd.DataFrame(columns=['Source IP', 'Environment'])

        # iterating through the given list of input files for transformation
        for filename in self.input_dataframes:
            # considering only source ip column as required
            each_dataframe = self.input_dataframes[filename][['Source IP']]
            each_dataframe = each_dataframe.drop_duplicates()

            # splitting and extracting the region and environment value from the file name
            details = filename.split(" ")
            text = details[0] + " " + details[1]

            # checking if the version value is missing
            if self.file_extension in text:
                text = text[:-4]
            each_dataframe['Environment'] = text

            # adding the content to the output dataframe
            self.output_dataframe = pd.concat([self.output_dataframe, each_dataframe])

        def sort_ip(df):
            """Sort the Source IP column of the dataframe"""

            df['ele1'] = df['Source IP'].str.split('.').str[0].astype('int')
            df['ele2'] = df['Source IP'].str.split('.').str[1].astype('int')
            df['ele3'] = df['Source IP'].str.split('.').str[2].astype('int')
            df['ele4'] = df['Source IP'].str.split('.').str[3].astype('int')
            df = df.sort_values(by=["ele1", "ele2", "ele3", "ele4"])
            df = df[['Source IP', 'Environment']]
            return df

        self.output_dataframe = sort_ip(self.output_dataframe)

    def load(self):
        """Write the output dataframe to the required output file"""

        path = self.path + '/' + self.output_file_name
        self.output_dataframe.to_csv('Combined.csv', index=False)


if __name__ == '__main__':
    job = SolutionUpload('.', 'Combined.csv')
    job.extract()
    print("input data read complete")
    job.empty_check()
    print("input folder is not empty")
    job.schema_match_check()
    print("input files schema are matching")
    job.source_ip_check()
    print("all files contain source IP column")
    job.ip_check()
    print("ip formats are proper")
    job.transform()
    print("data transformation complete")
    job.load()
    print("output data loaded and job complete")
