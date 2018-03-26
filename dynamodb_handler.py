from __future__ import print_function
from boto3.dynamodb.conditions import Key, Attr
import boto3
import json
import decimal
import sys

# Note - Do not change the class name and constructor
# You are free to add any functions to this class without changing the specifications mentioned below.
class DynamoDBHandler:

    def __init__(self, region):
        self.client = boto3.client('dynamodb')
        self.resource = boto3.resource('dynamodb', region_name=region, endpoint_url='http://localhost:8000')
        # self.resource = boto3.resource('dynamodb',endpoint_url="http://localhost:8000") 

    def help(self):
        print('Supported Commands:')
        print('1. insert_movie')
        print('2. delete_movie')
        print('3. update_movie')
        print('4. search_movie_actor')
        print('5. search_movie_actor_director')
        print('6. print_stats')
        print('7. delete_table')

    # query function for python2/3 input
    def io(self, query_string):
        if sys.version_info[0] < 3:
            user_input = raw_input(query_string)
        else:
            user_input = input(query_string)
        return user_input

    def create_and_load_data(self, tableName, fileName):
        # TODO - This function should create a table named <tableName> 
        # and load data from the file named <fileName>
        table = self.resource.create_table(
            TableName = tableName,
            KeySchema=[
                {
                    'AttributeName': 'year',
                    'KeyType': 'HASH' #Partition key
                },
                {
                    'AttributeName': 'title',
                    'KeyType': 'RANGE' #Sort key
                }
            ], 
            AttributeDefinitions=[
                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print('Table %s created' % tableName)

        # Wait until the table exists.
        waiter = self.client.get_waiter('table_not_exists')
        waiter.wait(
            TableName=tableName,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 10
            }
        )

        table = self.resource.Table(tableName)
        
        with open(fileName) as json_file:
            movies = json.load(json_file, parse_float = decimal.Decimal)
            for movie in movies:
                year = int(movie['year'])
                title = movie['title']
                info = movie['info']

                print('Adding movie:', year, title)

                table.put_item(
                    Item={
                        'year': year,
                        'title': title,
                        'info': info,
                    }
                )

        return None

    def insert_movie(self, year, title, director, actors, release_date, rating):
        print(year, title)
        tableName = 'Movies'
        table = self.resource.Table(tableName)
        response = table.put_item(
            Item={
                'year': year,
                'title': title,
                'director': director,
                'actors': actors,
                'release_Date': release_date,
                'rating': rating,
            }
        )
        return response


    def search_movie_actor(self, actor):
        tableName = 'Movies'
        table = self.resource.Table(tableName)
        response = table.scan(
            FilterExpression=Attr('actors').eq(actor)
        )
        return response['Items']


    def dispatch(self, command_string):
        # TODO - This function takes in as input a string command (e.g. 'insert_movie')
        # the return value of the function should depend on the command
        # For commands 'insert_movie', 'delete_movie', 'update_movie', delete_table' :
        #       return the message as a string that is expected as the output of the command
        # For commands 'search_movie_actor', 'search_movie_actor_director', print_stats' :
        #       return the a list of json objects where each json object has only the required
        #       keys and attributes of the expected result items.
        # Note: You should not print anything to the command line in this function.
        command = command_string.split(' ')
        response = ''

        if command[0] == 'insert_movie':
            year = int(self.io('Year> '))
            title = self.io('Title> ')
            director = self.io('Director> ')
            actors = self.io('Actors> ')
            release_date = self.io('Release Date> ')
            rating = self.io('Rating> ')
            if(year != '' or title != ''): # or director != '' or actors != '' or release_date != '' or rating != ''):
                response = self.insert_movie(year, title, director, actors, release_date, rating)
            else:
                response = "error"


        if command[0] == 'search_movie_actor':
            actor = self.io('Actor> ')
            if actor != '':
                response = self.search_movie_actor(actor)
            else:
                response = 'error'

        return response

def main():

    # check for correct number of arguments
    if (len(sys.argv) < 2 or len(sys.argv) > 2):
        print('Incorrect number of arguments. Use \'dynamodb_handler.py\' + [desired region] and try again.')
        print('See you soon!')
        exit()
    region = sys.argv[1]

    tableName = 'Movies'
    json_file = 'movies.json'

    # initialize handler with specified region
    dynamodb_handler = DynamoDBHandler(region)

    # Check if table/data has already been loaded
    try:
        dynamodb_handler.create_and_load_data(tableName, json_file)   # create table and laod data from file
    except dynamodb_handler.client.exceptions.ResourceInUseException:
        print('Table %s already exists' % tableName)

    while True:
        try:
            command = dynamodb_handler.io('Enter command (\'help\' to see all commands, \'exit\' to quit)> ')

            # Remove multiple whitespaces, if they exist
            command = ' '.join(command.split())

            if command == 'exit':
                print('Goodbye!')
                exit()
            elif command == 'help':
                dynamodb_handler.help()
            else:
                response = dynamodb_handler.dispatch(command)
                print(response)
        except Exception as e:
            print(e)
    

if __name__ == '__main__':
    main()
