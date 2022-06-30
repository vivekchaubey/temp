import os
import pandas as pd
import pytest


class SolutionUpload2:
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
        self.input_dataframes = {each: pd.read_csv(self.path+'/'+each) for each in input_csv_file_names}

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
        self.output_dataframe.to_csv(path, index=False)


if __name__ == '__main__':
    job = SolutionUpload2('/Users/vivekchaubey/Desktop/check', 'Combined.csv')
    job.extract()
    print("input data read complete")
    job.transform()
    print("data transformation complete")
    job.load()
    print("output data loaded and job complete")
