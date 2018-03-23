from __future__ import print_function
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

    # TODO: create query function for python2/3 input

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
        print('Created table')
        # Wait until the table exists.
        #table.meta.client.get_waiter('table_exists').wait(TableName=tableName)
        #print('Table status', table.item_count)
        '''waiter = self.client.get_waiter('table_exists')
        waiter.wait(
            TableName=tableName,
            #WaiterConfig={
             #   'Delay': 50,
              #  'MaxAttempts': 5
            #}
        )'''

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
            year = raw_input('Year> ')
            title = raw_input('Title> ')
            director = raw_input('Director> ')
            actors = raw_input('Actors> ')
            release_date = raw_input('Release Date> ')
            rating = raw_input('Rating> ')
            
        return response

def main():

    # check for correct number of arguments
    if (len(sys.argv) < 2 or len(sys.argv) > 2):
        print('Incorrect number of arguments. Use \'dynamodb_handler.py\' + [desired region] and try again.')
        print('See you soon!')
        exit()
    region = sys.argv[1]

    # initialize handler with specified region
    dynamodb_handler = DynamoDBHandler(region)

    # Check if table/data has already been loaded
    try:
        dynamodb_handler.create_and_load_data('Movies', 'moviedata.json')   # create table and laod data from file
    except dynamodb_handler.client.exceptions.ResourceInUseException:
        print('Table \'Movies\' already exists')

    while True:
        try:
            command = ''
            if sys.version_info[0] < 3:
                command = raw_input('Enter command (\'help\' to see all commands, \'exit\' to quit)> ')
            else:
                command = input('Enter command (\'help\' to see all commands, \'exit\' to quit)> ')

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
