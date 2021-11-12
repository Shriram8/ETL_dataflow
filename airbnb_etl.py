import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions,StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
import argparse
import logging
import csv



PROJECT_ID = 'meta-gear-331016'
JOB_NAME = 'neighbourhoodcountings1'
BUCKET_URL = 'gs://airbnb_nyc1'

#Function to read csv lines
def parse_file(element):
    
    for line in csv.reader([element],delimiter=','):
        return line


# Data Ingestion

def transform_method(values):

    row = dict(
        zip(('location', 'counts'),
            values))
    return row


def orignial_method(values):
    row1 = dict(
        zip(('id','name','host_id','host_name','neighbourhood_group','neighbourhood','latitude','longitude','room_type','price','minimum_nights','number_of_reviews','last_review','reviews_per_month','calculated_host_listings_count','availability_365'),
            values))
    return row1

#Class for Counting Listing per city
class CountListing(beam.DoFn):
    def process(self, element):
        # Returns a list of tuples containing Date and Open value
        if len(element)==16:
            result = [(element[5], int(element[14]))]
            return result


def run(argv=None, save_main_session=True):
    #Main entry point; defines and runs the wordcount pipeline.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://airbnb_nyc1/AB_NYC_2019.csv'
       )

    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        default='meta-gear-331016:ds1.listings_count')

    known_args, pipeline_args = parser.parse_known_args(argv)
    


    beam_options = PipelineOptions(    
        
    pipeline_args,
    runner = 'DataflowRunner',
    project= PROJECT_ID,
    job_name = JOB_NAME,
    temp_location = 'gs://airbnb_nyc1/temp/',
    region='us-east1',

    )

    beam_options.view_as(SetupOptions).save_main_session = save_main_session

    

    ### Creating Schema for Big Query Table1 - Listings table ####
    table_spec='listings_count'
    table_schema1 = {
            'fields':[{
                'name': 'location', 'type': 'STRING', 'mode': 'REQUIRED'
            }, {
                'name': 'counts', 'type': 'INT64', 'mode': 'REQUIRED'
            }]
        }

 
    ### Creating Schema for Big Query Table2 - original  table ####
    table_spec='meta-gear-331016:ds1.original_data'

    table_schema2 = {
        'fields':[
            {
                'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'
            }, 
            {
                'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'host_id', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'host_name', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'neighbourhood_group', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'neighbourhood', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'latitude', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'longitude', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'room_type', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'minimum_nights', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'number_of_reviews', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'last_review', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'reviews_per_month', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'calculated_host_listings_count', 'type': 'STRING', 'mode': 'NULLABLE'
            },
            {
                'name': 'availability_365', 'type': 'STRING', 'mode': 'NULLABLE'
            }
        ]}

  
    with beam.Pipeline(options=beam_options) as p:
        # Read the lines from CSV file
        lines = p |'Read' >> ReadFromText(known_args.input,skip_header_lines=1) | 'Parse file' >> beam.Map(parse_file) 

        output= lines | 'Count'>> beam.ParDo(CountListing()) | 'GroupBY'>> beam.CombinePerKey(sum) 
        
        transformed_data=output | 'Ingesting Data for Big Query'>> beam.Map(lambda s: transform_method(s))

        #Creating Pipline for pushing transformed Data into BigQuery

        transformed_data | 'Writing Data to Big Query'>> beam.io.WriteToBigQuery(
                            known_args.output,
                            schema=table_schema1,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        #Creating Pipline for pushing Original Data into BigQuery

        original_data=lines | 'Ingesting Original Data for Big Query'>> beam.Map(lambda s: orignial_method(s))

        original_data | 'Writing Original Data to Big Query'>> beam.io.WriteToBigQuery(
                    table_spec,
                    schema=table_schema2,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    return


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    